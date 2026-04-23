defmodule Aerospike.Command.GetRetryTest do
  @moduledoc """
  Unit tests for the retry driver exercised through
  `Aerospike.Command.Get.execute/4`.

  The tests script the Fake transport so the command path consumes one
  scripted AS_MSG reply per attempt and assert the retry loop's
  classification, deadline, and replica-rotation behaviour without
  relying on timing side channels.
  """

  use ExUnit.Case, async: false

  alias Aerospike.Cluster.NodeCounters
  alias Aerospike.Cluster.NodeSupervisor
  alias Aerospike.Cluster.PartitionMap
  alias Aerospike.Cluster.PartitionMapWriter
  alias Aerospike.Cluster.TableOwner
  alias Aerospike.Cluster.Tender
  alias Aerospike.Command.Get
  alias Aerospike.Error
  alias Aerospike.Key
  alias Aerospike.Telemetry
  alias Aerospike.Test.ReplicasFixture
  alias Aerospike.Transport.Fake

  @namespace "test"
  @set "spike"
  @user_key "any"
  # Partition id derived from `Key.new(@namespace, @set, @user_key)`.
  # Asserted below in `setup` so a future RIPEMD change surfaces here.
  @partition_id 474

  setup context do
    name = :"get_retry_#{:erlang.phash2(context.test)}"

    # Two fake nodes so `:sequence` has somewhere to rotate to.
    {:ok, fake} =
      Fake.start_link(
        nodes: [
          {"A1", "10.0.0.1", 3000},
          {"B1", "10.0.0.2", 3000}
        ]
      )

    {:ok, owner} = TableOwner.start_link(name: name)
    tables = TableOwner.tables(owner)
    {:ok, writer} = PartitionMapWriter.start_link(name: name, tables: tables)
    {:ok, node_sup} = NodeSupervisor.start_link(name: name)

    on_exit(fn ->
      stop_quietly(node_sup)
      stop_quietly(writer)
      stop_quietly(owner)
      stop_quietly(fake)
    end)

    # Sanity check: if the partition id for the canonical spike key ever
    # shifts, the multi-replica scripting below is wrong. Fail fast.
    assert Key.partition_id(Key.new(@namespace, @set, @user_key)) == @partition_id

    %{
      name: name,
      fake: fake,
      owner: owner,
      tables: tables,
      node_sup_name: NodeSupervisor.sup_name(name)
    }
  end

  describe ":sequence replica rotation" do
    test "transport error on attempt 0 retries on the next replica", ctx do
      script_two_replica_cluster(ctx.fake)

      {:ok, tender} = start_tender(ctx, replica_policy: :sequence, max_retries: 2)
      :ok = Tender.tend_now(tender)

      # Confirm the partition map has A1 master, B1 secondary at :sequence.
      {:ok, po} = PartitionMap.owners(ctx.tables.owners, @namespace, @partition_id)
      assert po.replicas == ["A1", "B1"]

      # Attempt 0 → A1: transport failure. Attempt 1 → B1: success.
      Fake.script_command(ctx.fake, "A1", {:error, %Error{code: :network_error, message: "fake"}})
      Fake.script_command(ctx.fake, "B1", {:ok, scripted_ok_body()})

      key = Key.new(@namespace, @set, @user_key)
      assert {:ok, record} = Get.execute(tender, key, :all)
      assert record.key.namespace == @namespace
    end

    test "circuit-open refusal is retryable and rotates to the next replica", ctx do
      script_two_replica_cluster(ctx.fake)

      {:ok, tender} =
        start_tender(ctx,
          replica_policy: :sequence,
          max_retries: 2,
          circuit_open_threshold: 1
        )

      :ok = Tender.tend_now(tender)

      # Trip A1's breaker by bumping its failure counter above the threshold.
      {:ok, counters} = Tender.node_counters(tender, "A1")
      NodeCounters.incr_failed(counters)

      # Attempt 0 against A1 is refused by the breaker before touching the
      # pool; attempt 1 rotates to B1 which returns a success reply.
      Fake.script_command(ctx.fake, "B1", {:ok, scripted_ok_body()})

      key = Key.new(@namespace, @set, @user_key)
      assert {:ok, _record} = Get.execute(tender, key, :all)
    end

    test ":max_retries: 0 disables retry entirely", ctx do
      script_two_replica_cluster(ctx.fake)

      {:ok, tender} = start_tender(ctx, replica_policy: :sequence)
      :ok = Tender.tend_now(tender)

      Fake.script_command(ctx.fake, "A1", {:error, %Error{code: :network_error, message: "fake"}})
      # Not scripted on B1 on purpose: if the driver retried, the second
      # call would consume the Fake's default :no_script reply and leak
      # that into the assertion.
      key = Key.new(@namespace, @set, @user_key)

      assert {:error, %Error{code: :network_error, message: "fake"}} =
               Get.execute(tender, key, :all, max_retries: 0)
    end

    test "exhausting :max_retries returns the most recent error", ctx do
      script_two_replica_cluster(ctx.fake)

      {:ok, tender} = start_tender(ctx, replica_policy: :sequence)
      :ok = Tender.tend_now(tender)

      # Every attempt sees a transport error. With :max_retries: 1 the
      # driver makes two attempts total (A1, then B1 via :sequence).
      Fake.script_command(ctx.fake, "A1", {:error, %Error{code: :network_error, message: "fake"}})
      Fake.script_command(ctx.fake, "B1", {:error, %Error{code: :timeout, message: "fake"}})

      key = Key.new(@namespace, @set, @user_key)

      assert {:error, %Error{code: :timeout, message: "fake"}} =
               Get.execute(tender, key, :all, max_retries: 1)
    end
  end

  describe "rebalance classification" do
    test "PARTITION_UNAVAILABLE triggers an async tend and retries", ctx do
      # Two-replica map on cycle 1; a third cycle script is ready but the
      # test does not assert on map changes — it only asserts the retry
      # re-dispatches to the next replica after the rebalance signal.
      script_two_replica_cluster(ctx.fake)

      {:ok, tender} = start_tender(ctx, replica_policy: :sequence, max_retries: 2)
      :ok = Tender.tend_now(tender)

      # Attempt 0 → A1 replies with PARTITION_UNAVAILABLE (result code 11).
      # Attempt 1 → B1 replies with a record.
      Fake.script_command(ctx.fake, "A1", {:ok, scripted_rebalance_body()})
      Fake.script_command(ctx.fake, "B1", {:ok, scripted_ok_body()})

      key = Key.new(@namespace, @set, @user_key)
      assert {:ok, _record} = Get.execute(tender, key, :all)
    end
  end

  describe ":replica_policy: :master" do
    test "pins every attempt to the master replica", ctx do
      script_two_replica_cluster(ctx.fake)

      {:ok, tender} = start_tender(ctx, replica_policy: :master, max_retries: 2)
      :ok = Tender.tend_now(tender)

      # Every attempt must hit A1. Script two transport errors on A1 then
      # a success; B1 has no command script, so if the driver were to
      # rotate, the third attempt would see `:no_script` and the
      # assertion below would fail.
      Fake.script_command(ctx.fake, "A1", {:error, %Error{code: :network_error, message: "fake"}})
      Fake.script_command(ctx.fake, "A1", {:error, %Error{code: :network_error, message: "fake"}})
      Fake.script_command(ctx.fake, "A1", {:ok, scripted_ok_body()})

      key = Key.new(@namespace, @set, @user_key)
      assert {:ok, _record} = Get.execute(tender, key, :all)
    end
  end

  describe "budget / deadline" do
    test "deadline exhaustion returns the most recent transport error", ctx do
      script_two_replica_cluster(ctx.fake)

      {:ok, tender} =
        start_tender(ctx, replica_policy: :sequence, sleep_between_retries_ms: 25)

      :ok = Tender.tend_now(tender)

      Fake.script_command(ctx.fake, "A1", {:error, %Error{code: :network_error, message: "fake"}})
      Fake.script_command(ctx.fake, "B1", {:error, %Error{code: :network_error, message: "fake"}})

      # :timeout is 5ms: the first attempt returns, then the 25ms sleep
      # between retries blows the deadline before attempt 1 is issued.
      key = Key.new(@namespace, @set, @user_key)

      assert {:error, %Error{code: :network_error, message: "fake"}} =
               Get.execute(tender, key, :all, timeout: 5, max_retries: 3)
    end
  end

  describe "per-call overrides vs cluster defaults" do
    test "per-call :replica_policy overrides the cluster default", ctx do
      script_two_replica_cluster(ctx.fake)

      # Cluster default is :master. The call overrides to :sequence so
      # attempt 1 rotates from A1 to B1 instead of re-pinning A1.
      {:ok, tender} = start_tender(ctx, replica_policy: :master)
      :ok = Tender.tend_now(tender)

      Fake.script_command(ctx.fake, "A1", {:error, %Error{code: :network_error, message: "fake"}})
      Fake.script_command(ctx.fake, "B1", {:ok, scripted_ok_body()})

      key = Key.new(@namespace, @set, @user_key)

      assert {:ok, _record} =
               Get.execute(tender, key, :all, replica_policy: :sequence, max_retries: 1)
    end

    test "per-call :max_retries overrides the cluster default downward", ctx do
      script_two_replica_cluster(ctx.fake)

      # Cluster default max_retries: 5 — call clamps to 0.
      {:ok, tender} = start_tender(ctx, replica_policy: :sequence, max_retries: 5)
      :ok = Tender.tend_now(tender)

      Fake.script_command(ctx.fake, "A1", {:error, %Error{code: :network_error, message: "fake"}})

      key = Key.new(@namespace, @set, @user_key)

      assert {:error, %Error{code: :network_error, message: "fake"}} =
               Get.execute(tender, key, :all, max_retries: 0)
    end
  end

  describe "retry telemetry" do
    test "emits :retry_attempt with :transport classification on transport retry", ctx do
      script_two_replica_cluster(ctx.fake)

      {:ok, tender} = start_tender(ctx, replica_policy: :sequence, max_retries: 2)
      :ok = Tender.tend_now(tender)

      Fake.script_command(ctx.fake, "A1", {:error, %Error{code: :network_error, message: "fake"}})
      Fake.script_command(ctx.fake, "B1", {:ok, scripted_ok_body()})

      handler = attach_retry_handler(:transport_retry)

      try do
        key = Key.new(@namespace, @set, @user_key)
        assert {:ok, _record} = Get.execute(tender, key, :all)

        assert_receive {:event, [:aerospike, :retry, :attempt], %{remaining_budget_ms: budget},
                        %{classification: :transport, attempt: 1, node_name: "A1"}},
                       500

        assert is_integer(budget) and budget >= 0
      after
        :telemetry.detach(handler)
      end
    end

    test "emits :retry_attempt with :rebalance classification on PARTITION_UNAVAILABLE", ctx do
      script_two_replica_cluster(ctx.fake)

      {:ok, tender} = start_tender(ctx, replica_policy: :sequence, max_retries: 2)
      :ok = Tender.tend_now(tender)

      Fake.script_command(ctx.fake, "A1", {:ok, scripted_rebalance_body()})
      Fake.script_command(ctx.fake, "B1", {:ok, scripted_ok_body()})

      handler = attach_retry_handler(:rebalance_retry)

      try do
        key = Key.new(@namespace, @set, @user_key)
        assert {:ok, _record} = Get.execute(tender, key, :all)

        assert_receive {:event, [:aerospike, :retry, :attempt], _m,
                        %{classification: :rebalance, attempt: 1, node_name: "A1"}},
                       500
      after
        :telemetry.detach(handler)
      end
    end

    test "emits :retry_attempt with :circuit_open classification when breaker refuses", ctx do
      script_two_replica_cluster(ctx.fake)

      {:ok, tender} =
        start_tender(ctx,
          replica_policy: :sequence,
          max_retries: 2,
          circuit_open_threshold: 1
        )

      :ok = Tender.tend_now(tender)

      {:ok, counters} = Tender.node_counters(tender, "A1")
      NodeCounters.incr_failed(counters)

      Fake.script_command(ctx.fake, "B1", {:ok, scripted_ok_body()})

      handler = attach_retry_handler(:circuit_retry)

      try do
        key = Key.new(@namespace, @set, @user_key)
        assert {:ok, _record} = Get.execute(tender, key, :all)

        assert_receive {:event, [:aerospike, :retry, :attempt], _m,
                        %{classification: :circuit_open, attempt: 1, node_name: "A1"}},
                       500
      after
        :telemetry.detach(handler)
      end
    end

    test "does not emit :retry_attempt on the first successful attempt", ctx do
      script_two_replica_cluster(ctx.fake)

      {:ok, tender} = start_tender(ctx, replica_policy: :sequence, max_retries: 2)
      :ok = Tender.tend_now(tender)

      Fake.script_command(ctx.fake, "A1", {:ok, scripted_ok_body()})

      handler = attach_retry_handler(:no_retry)

      try do
        key = Key.new(@namespace, @set, @user_key)
        assert {:ok, _record} = Get.execute(tender, key, :all)

        refute_receive {:event, [:aerospike, :retry, :attempt], _m, _meta}, 100
      after
        :telemetry.detach(handler)
      end
    end
  end

  # Captured forwarder to avoid the "local function handler" performance
  # warning from `:telemetry.attach/4` for anonymous captures.
  @doc false
  def forward(event, measurements, metadata, test_pid) do
    send(test_pid, {:event, event, measurements, metadata})
  end

  defp attach_retry_handler(tag) do
    handler_id = {__MODULE__, tag, make_ref()}

    :ok =
      :telemetry.attach(
        handler_id,
        Telemetry.retry_attempt(),
        &__MODULE__.forward/4,
        self()
      )

    handler_id
  end

  ## Helpers

  # Builds a two-node, two-replica cluster view: A1 is master and B1 is
  # secondary for every partition at regime 1. Both nodes are bootstrapped
  # as seeds; peers discovery is empty on both so no further nodes are
  # pulled in.
  defp script_two_replica_cluster(fake) do
    partitions = Enum.to_list(0..(PartitionMap.partition_count() - 1))

    # A1 owns every partition at replica slot 0 (master).
    a1_replicas = ReplicasFixture.build(@namespace, 1, [partitions, []])
    # B1 owns every partition at replica slot 1 (secondary).
    b1_replicas = ReplicasFixture.build(@namespace, 1, [[], partitions])

    Fake.script_info(fake, "A1", ["node", "features"], %{"node" => "A1", "features" => ""})
    Fake.script_info(fake, "B1", ["node", "features"], %{"node" => "B1", "features" => ""})

    script_cycle(fake, "A1", gen: 1, peers: "0,3000,[]", replicas: a1_replicas)
    script_cycle(fake, "B1", gen: 1, peers: "0,3000,[]", replicas: b1_replicas)
  end

  defp script_cycle(fake, node_name, opts) do
    Fake.script_info(
      fake,
      node_name,
      ["partition-generation", "cluster-stable", "peers-generation"],
      %{
        "partition-generation" => Integer.to_string(Keyword.fetch!(opts, :gen)),
        "cluster-stable" => Keyword.get(opts, :cluster_stable, "deadbeef"),
        "peers-generation" => "1"
      }
    )

    Fake.script_info(fake, node_name, ["peers-clear-std"], %{
      "peers-clear-std" => Keyword.fetch!(opts, :peers)
    })

    Fake.script_info(fake, node_name, ["replicas"], %{
      "replicas" => Keyword.fetch!(opts, :replicas)
    })
  end

  defp start_tender(ctx, overrides) do
    base_opts = [
      name: ctx.name,
      transport: Fake,
      connect_opts: [fake: ctx.fake],
      seeds: [{"10.0.0.1", 3000}, {"10.0.0.2", 3000}],
      namespaces: [@namespace],
      tables: ctx.tables,
      tend_trigger: :manual,
      node_supervisor: ctx.node_sup_name,
      pool_size: 1
    ]

    {:ok, pid} = Tender.start_link(Keyword.merge(base_opts, overrides))
    on_exit(fn -> stop_quietly(pid) end)
    {:ok, pid}
  end

  # AS_MSG body for a :key_found reply with zero bins. Result code 0 and
  # zero ops is the shape `Response.parse_record_response/2` returns as
  # `{:ok, %Record{bins: %{}}}`.
  defp scripted_ok_body do
    <<22, 0, 0, 0, 0, 0::8, 0::32, 0::32, 0::32, 0::16, 0::16>>
  end

  # AS_MSG body for a PARTITION_UNAVAILABLE reply (result code 11).
  # `Aerospike.Error.rebalance?/1` classifies this as a rebalance signal.
  defp scripted_rebalance_body do
    <<22, 0, 0, 0, 0, 11::8, 0::32, 0::32, 0::32, 0::16, 0::16>>
  end

  defp stop_quietly(pid) do
    if Process.alive?(pid) do
      ref = Process.monitor(pid)
      Process.exit(pid, :shutdown)

      receive do
        {:DOWN, ^ref, _, _, _} -> :ok
      after
        1_000 -> :ok
      end
    end
  end
end
