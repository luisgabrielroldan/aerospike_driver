defmodule Aerospike.WriteFamilyTest do
  use ExUnit.Case, async: true

  alias Aerospike.Delete
  alias Aerospike.Error
  alias Aerospike.Exists
  alias Aerospike.Key
  alias Aerospike.NodeSupervisor
  alias Aerospike.PartitionMap
  alias Aerospike.PartitionMapWriter
  alias Aerospike.Put
  alias Aerospike.TableOwner
  alias Aerospike.Tender
  alias Aerospike.Test.ReplicasFixture
  alias Aerospike.Touch
  alias Aerospike.Transport.Fake

  @namespace "test"
  @set "spike"

  setup context do
    name = :"write_family_#{:erlang.phash2(context.test)}"

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

    %{
      fake: fake,
      name: name,
      node_sup_name: NodeSupervisor.sup_name(name),
      tables: tables
    }
  end

  describe "put" do
    test "returns metadata for a successful write", ctx do
      script_bootstrap_node(ctx.fake, "A1", ReplicasFixture.all_master(@namespace, 1))
      {:ok, tender} = start_tender(ctx, seeds: [{"10.0.0.1", 3000}])
      :ok = Tender.tend_now(tender)

      Fake.script_command(ctx.fake, "A1", {:ok, scripted_reply_body(0, 3, 120)})

      key = Key.new(@namespace, @set, "put-success")

      assert {:ok, %{generation: 3, ttl: 120}} =
               Put.execute(tender, key, %{count: 7}, ttl: 120)
    end

    test "retries transport errors on the write master through the shared unary path", ctx do
      script_bootstrap_node(ctx.fake, "A1", ReplicasFixture.all_master(@namespace, 1))
      {:ok, tender} = start_tender(ctx, seeds: [{"10.0.0.1", 3000}], max_retries: 1)
      :ok = Tender.tend_now(tender)

      Fake.script_command(ctx.fake, "A1", {:error, Error.from_result_code(:network_error)})
      Fake.script_command(ctx.fake, "A1", {:ok, scripted_reply_body(0, 2, 33)})

      key = Key.new(@namespace, @set, "put-retry")

      assert {:ok, %{generation: 2, ttl: 33}} =
               Aerospike.put(tender, key, %{count: 9})

      assert Keyword.get(Fake.last_command_opts(ctx.fake, "A1"), :attempt) == 1
    end

    test "returns routing refusal before the cluster is ready", ctx do
      script_bootstrap_node(ctx.fake, "A1", ReplicasFixture.all_master(@namespace, 1))
      {:ok, tender} = start_tender(ctx, seeds: [{"10.0.0.1", 3000}])

      key = Key.new(@namespace, @set, "put-not-ready")

      assert {:error, :cluster_not_ready} = Put.execute(tender, key, %{count: 1})
    end
  end

  describe "exists" do
    test "uses read routing and can rotate to the next replica under :sequence", ctx do
      script_two_replica_cluster(ctx.fake)
      {:ok, tender} = start_tender(ctx, replica_policy: :sequence, max_retries: 1)
      :ok = Tender.tend_now(tender)

      Fake.script_command(ctx.fake, "A1", {:error, Error.from_result_code(:network_error)})
      Fake.script_command(ctx.fake, "B1", {:ok, scripted_reply_body(0, 4, 88)})

      key = Key.new(@namespace, @set, "exists-retry")

      assert {:ok, true} = Exists.execute(tender, key)
      assert Keyword.get(Fake.last_command_opts(ctx.fake, "B1"), :attempt) == 1
    end

    test "returns false on key-not-found without reading bins", ctx do
      script_bootstrap_node(ctx.fake, "A1", ReplicasFixture.all_master(@namespace, 1))
      {:ok, tender} = start_tender(ctx, seeds: [{"10.0.0.1", 3000}])
      :ok = Tender.tend_now(tender)

      Fake.script_command(ctx.fake, "A1", {:ok, scripted_reply_body(2)})

      key = Key.new(@namespace, @set, "exists-missing")

      assert {:ok, false} = Aerospike.exists(tender, key)
    end
  end

  describe "delete" do
    test "returns false when the key is already missing", ctx do
      script_bootstrap_node(ctx.fake, "A1", ReplicasFixture.all_master(@namespace, 1))
      {:ok, tender} = start_tender(ctx, seeds: [{"10.0.0.1", 3000}])
      :ok = Tender.tend_now(tender)

      Fake.script_command(ctx.fake, "A1", {:ok, scripted_reply_body(2)})

      key = Key.new(@namespace, @set, "delete-missing")

      assert {:ok, false} = Delete.execute(tender, key)
    end
  end

  describe "touch" do
    test "returns key_not_found for missing records instead of assuming success", ctx do
      script_bootstrap_node(ctx.fake, "A1", ReplicasFixture.all_master(@namespace, 1))
      {:ok, tender} = start_tender(ctx, seeds: [{"10.0.0.1", 3000}])
      :ok = Tender.tend_now(tender)

      Fake.script_command(ctx.fake, "A1", {:ok, scripted_reply_body(2)})

      key = Key.new(@namespace, @set, "touch-missing")

      assert {:error, %Error{code: :key_not_found}} = Touch.execute(tender, key)
    end
  end

  defp script_bootstrap_node(fake, node_name, replicas_value) do
    Fake.script_info(fake, node_name, ["node", "features"], %{
      "node" => node_name,
      "features" => ""
    })

    script_cycle(fake, node_name, gen: 1, peers: "0,3000,[]", replicas: replicas_value)
  end

  defp script_two_replica_cluster(fake) do
    partitions = Enum.to_list(0..(PartitionMap.partition_count() - 1))
    a1_replicas = ReplicasFixture.build(@namespace, 1, [partitions, []])
    b1_replicas = ReplicasFixture.build(@namespace, 1, [[], partitions])

    script_bootstrap_node(fake, "A1", a1_replicas)
    script_bootstrap_node(fake, "B1", b1_replicas)
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

  defp scripted_reply_body(result_code, generation \\ 0, ttl \\ 0) do
    <<22, 0, 0, 0, 0, result_code::8, generation::32-big, ttl::32-big, 0::32, 0::16, 0::16>>
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
