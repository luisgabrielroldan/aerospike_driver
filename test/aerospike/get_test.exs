defmodule Aerospike.Command.GetTest do
  @moduledoc """
  Unit tests for `Aerospike.Command.Get.execute/4` that cover decisions the
  command path makes *before* it touches the transport.

  Transport-level paths (encode, decode, error classification) are
  covered by integration tests and the per-module protocol tests; this
  file asserts only that `Aerospike.Command.Get` refuses to check out a pool
  worker when the breaker refuses.
  """

  use ExUnit.Case, async: true

  alias Aerospike
  alias Aerospike.Cluster.NodeCounters
  alias Aerospike.Cluster.NodeSupervisor
  alias Aerospike.Cluster.PartitionMapWriter
  alias Aerospike.Cluster.TableOwner
  alias Aerospike.Cluster.Tender
  alias Aerospike.Command.Get
  alias Aerospike.Error
  alias Aerospike.Key
  alias Aerospike.Test.ReplicasFixture
  alias Aerospike.Transport.Fake

  setup context do
    name = :"get_test_#{:erlang.phash2(context.test)}"

    {:ok, fake} = Fake.start_link(nodes: [{"A1", "10.0.0.1", 3000}])
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

    %{
      name: name,
      fake: fake,
      owner: owner,
      tables: tables,
      node_sup_name: NodeSupervisor.sup_name(name)
    }
  end

  describe "circuit-breaker short-circuit" do
    test "returns :circuit_open without reaching the pool when the failure cap is hit", ctx do
      script_bootstrap_node(ctx.fake, "A1", 1, ReplicasFixture.all_master("test", 1))

      {:ok, tender} = start_tender(ctx, circuit_open_threshold: 2)
      :ok = Tender.tend_now(tender)

      {:ok, counters} = Tender.node_counters(tender, "A1")

      # Force the failure counter to the cap. The breaker re-reads on
      # every call so the next Get attempt sees the open circuit.
      NodeCounters.incr_failed(counters)
      NodeCounters.incr_failed(counters)

      # No command scripted on the Fake: if the breaker did not short-
      # circuit, the command path would block waiting for a reply that
      # never arrives (or the Fake would raise on an unscripted call).
      # The test asserts on the error surface the retry layer reads.
      key = Key.new("test", "spike", "any")
      assert {:error, %Error{code: :circuit_open}} = Get.execute(tender, key, :all)
    end

    test "resumes normal dispatch after the Tender decays the failure counter", ctx do
      script_bootstrap_node(ctx.fake, "A1", 1, ReplicasFixture.all_master("test", 1))

      # Cycle 2 — a healthy refresh-node info call decays `:failed` to 0.
      script_cycle(ctx.fake, "A1",
        gen: 2,
        peers: "0,3000,[]",
        replicas: ReplicasFixture.all_master("test", 2)
      )

      {:ok, tender} = start_tender(ctx, circuit_open_threshold: 2)
      :ok = Tender.tend_now(tender)

      {:ok, counters} = Tender.node_counters(tender, "A1")
      NodeCounters.incr_failed(counters)
      NodeCounters.incr_failed(counters)

      key = Key.new("test", "spike", "any")
      assert {:error, %Error{code: :circuit_open}} = Get.execute(tender, key, :all)

      # Cycle 2 decays the counter; the breaker now admits the attempt.
      # Script a command reply so the decoded-path actually completes.
      # The server replies with :key_not_found for a missing record,
      # which is the normal shape Get returns.
      Fake.script_command(ctx.fake, "A1", scripted_key_not_found_body())
      :ok = Tender.tend_now(tender)

      assert {:error, %Error{code: :key_not_found}} = Get.execute(tender, key, :all)
    end

    test "returns :circuit_open when the concurrency cap is exhausted", ctx do
      script_bootstrap_node(ctx.fake, "A1", 1, ReplicasFixture.all_master("test", 1))

      {:ok, tender} =
        start_tender(ctx,
          circuit_open_threshold: 100,
          max_concurrent_ops_per_node: 2
        )

      :ok = Tender.tend_now(tender)

      {:ok, counters} = Tender.node_counters(tender, "A1")
      # Two synthetic in-flight ops — the third attempt trips the cap.
      NodeCounters.incr_in_flight(counters)
      NodeCounters.incr_in_flight(counters)

      key = Key.new("test", "spike", "any")
      assert {:error, %Error{code: :circuit_open}} = Get.execute(tender, key, :all)
    end
  end

  describe "unsupported GET shapes" do
    test "Get.execute/4 rejects named-bin requests in the spike" do
      key = Key.new("test", "spike", "any")

      assert {:error, %Error{code: :invalid_argument}} =
               Get.execute(:unused_tender, key, ["bin_a"])
    end

    test "Aerospike.get/4 preserves the same named-bin rejection surface" do
      key = Key.new("test", "spike", "any")

      assert {:error, %Error{code: :invalid_argument}} =
               Aerospike.get(:unused_cluster, key, ["bin_a"])
    end
  end

  describe ":use_compression plumbing" do
    # The matrix of (cluster flag, node feature) that decides whether a
    # command is dispatched with `use_compression: true`. The Fake
    # transport captures the opts passed to each `command/4` call so we
    # can assert the boolean without touching the wire path.

    test "cluster flag off + node advertises :compression → command sent plain", ctx do
      script_bootstrap_with_features(ctx.fake, "A1", "compression;pipelining")

      {:ok, tender} = start_tender(ctx, use_compression: false)
      :ok = Tender.tend_now(tender)

      Fake.script_command(ctx.fake, "A1", scripted_key_not_found_body())

      key = Key.new("test", "spike", "any")
      assert {:error, %Error{code: :key_not_found}} = Get.execute(tender, key, :all)

      opts = Fake.last_command_opts(ctx.fake, "A1")
      assert Keyword.get(opts, :use_compression) == false
    end

    test "cluster flag on + node lacks :compression → command sent plain", ctx do
      script_bootstrap_with_features(ctx.fake, "A1", "pipelining")

      {:ok, tender} = start_tender(ctx, use_compression: true)
      :ok = Tender.tend_now(tender)

      Fake.script_command(ctx.fake, "A1", scripted_key_not_found_body())

      key = Key.new("test", "spike", "any")
      assert {:error, %Error{code: :key_not_found}} = Get.execute(tender, key, :all)

      opts = Fake.last_command_opts(ctx.fake, "A1")
      assert Keyword.get(opts, :use_compression) == false
    end

    test "cluster flag on + node advertises :compression → command sent with flag true", ctx do
      script_bootstrap_with_features(ctx.fake, "A1", "compression;pipelining")

      {:ok, tender} = start_tender(ctx, use_compression: true)
      :ok = Tender.tend_now(tender)

      Fake.script_command(ctx.fake, "A1", scripted_key_not_found_body())

      key = Key.new("test", "spike", "any")
      assert {:error, %Error{code: :key_not_found}} = Get.execute(tender, key, :all)

      opts = Fake.last_command_opts(ctx.fake, "A1")
      assert Keyword.get(opts, :use_compression) == true
    end
  end

  ## Helpers

  defp start_tender(ctx, opts) do
    tender_opts =
      [
        name: ctx.name,
        transport: Fake,
        connect_opts: [fake: ctx.fake],
        seeds: [{"10.0.0.1", 3000}],
        namespaces: ["test"],
        tables: ctx.tables,
        tend_trigger: :manual,
        node_supervisor: ctx.node_sup_name,
        pool_size: 1
      ]
      |> maybe_put(:circuit_open_threshold, Keyword.get(opts, :circuit_open_threshold))
      |> maybe_put(
        :max_concurrent_ops_per_node,
        Keyword.get(opts, :max_concurrent_ops_per_node)
      )
      |> maybe_put(:use_compression, Keyword.get(opts, :use_compression))

    {:ok, pid} = Tender.start_link(tender_opts)
    on_exit(fn -> stop_quietly(pid) end)
    {:ok, pid}
  end

  defp maybe_put(opts, _key, nil), do: opts
  defp maybe_put(opts, key, value), do: Keyword.put(opts, key, value)

  defp script_bootstrap_node(fake, node_name, partition_gen, replicas_value) do
    Fake.script_info(fake, node_name, ["node", "features"], %{
      "node" => node_name,
      "features" => ""
    })

    script_cycle(fake, node_name,
      gen: partition_gen,
      peers: "0,3000,[]",
      replicas: replicas_value
    )
  end

  defp script_bootstrap_with_features(fake, node_name, features) do
    Fake.script_info(fake, node_name, ["node", "features"], %{
      "node" => node_name,
      "features" => features
    })

    script_cycle(fake, node_name,
      gen: 1,
      peers: "0,3000,[]",
      replicas: ReplicasFixture.all_master("test", 1)
    )
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

  # Minimal AS_MSG body for a `key_not_found` reply. The parser reads
  # the result code from byte offset 13; everything else is zero.
  defp scripted_key_not_found_body do
    # 22-byte AS_MSG header with result code 2 (:key_not_found) at
    # offset 13. Field count = 0, op count = 0. Generation, ttl, etc
    # are irrelevant for this code path.
    {:ok, header()}
  end

  defp header do
    # header format (22 bytes):
    #   1 byte  header_sz = 22
    #   1 byte  info1
    #   1 byte  info2
    #   1 byte  info3
    #   1 byte  unused
    #   1 byte  result_code
    #   4 bytes generation
    #   4 bytes record_ttl
    #   4 bytes transaction_ttl
    #   2 bytes n_fields
    #   2 bytes n_ops
    <<22, 0, 0, 0, 0, 2::8, 0::32, 0::32, 0::32, 0::16, 0::16>>
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
