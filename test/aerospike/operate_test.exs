defmodule Aerospike.OperateTest do
  use ExUnit.Case, async: true

  alias Aerospike.Error
  alias Aerospike.Key
  alias Aerospike.NodeSupervisor
  alias Aerospike.Op
  alias Aerospike.Operate
  alias Aerospike.Protocol.AsmMsg
  alias Aerospike.Record
  alias Aerospike.TableOwner
  alias Aerospike.Tender
  alias Aerospike.Test.ReplicasFixture
  alias Aerospike.Transport.Fake

  @namespace "test"
  @set "spike"

  setup context do
    name = :"operate_#{:erlang.phash2(context.test)}"

    {:ok, fake} =
      Fake.start_link(
        nodes: [
          {"A1", "10.0.0.1", 3000},
          {"B1", "10.0.0.2", 3000}
        ]
      )

    {:ok, owner} = TableOwner.start_link(name: name)
    tables = TableOwner.tables(owner)
    {:ok, node_sup} = NodeSupervisor.start_link(name: name)

    on_exit(fn ->
      stop_quietly(node_sup)
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

  describe "operate" do
    test "returns the read result for mixed write/read operations", ctx do
      script_bootstrap_node(ctx.fake, "A1", ReplicasFixture.all_master(@namespace, 1))
      {:ok, tender} = start_tender(ctx, seeds: [{"10.0.0.1", 3000}])
      :ok = Tender.tend_now(tender)

      Fake.script_command(ctx.fake, "A1", {:ok, operate_reply([7], ["count"])})

      key = Key.new(@namespace, @set, "operate-success")

      assert {:ok, %Record{bins: %{"count" => 7}}} =
               Aerospike.operate(
                 tender,
                 key,
                 [{:write, :count, 7}, {:read, "count"}],
                 ttl: 30
               )
    end

    test "admits touch, delete, add, append, and prepend operations", ctx do
      script_bootstrap_node(ctx.fake, "A1", ReplicasFixture.all_master(@namespace, 1))
      {:ok, tender} = start_tender(ctx, seeds: [{"10.0.0.1", 3000}], max_retries: 1)
      :ok = Tender.tend_now(tender)

      for _ <- 1..5 do
        Fake.script_command(ctx.fake, "A1", {:ok, operate_reply([], [])})
      end

      assert {:ok, %Record{}} =
               Operate.execute(tender, Key.new(@namespace, @set, "touch"), [:touch])

      assert {:ok, %Record{}} =
               Operate.execute(tender, Key.new(@namespace, @set, "delete"), [:delete])

      assert {:ok, %Record{}} =
               Operate.execute(tender, Key.new(@namespace, @set, "add"), [{:add, "count", 11}])

      assert {:ok, %Record{}} =
               Operate.execute(
                 tender,
                 Key.new(@namespace, @set, "append"),
                 [{:append, "name", "x"}]
               )

      assert {:ok, %Record{}} =
               Operate.execute(
                 tender,
                 Key.new(@namespace, @set, "prepend"),
                 [{:prepend, "name", "y"}]
               )
    end

    test "accepts read-only helper operations through the shared unary path", ctx do
      script_bootstrap_node(ctx.fake, "A1", ReplicasFixture.all_master(@namespace, 1))
      {:ok, tender} = start_tender(ctx, seeds: [{"10.0.0.1", 3000}])
      :ok = Tender.tend_now(tender)

      Fake.script_command(ctx.fake, "A1", {:ok, operate_reply([7], ["count"])})

      key = Key.new(@namespace, @set, "operate-read-only")

      assert {:ok, %Record{bins: %{"count" => 7}}} =
               Aerospike.operate(tender, key, [Op.get("count")])
    end

    test "accepts CDT helper operations through the shared unary path", ctx do
      script_bootstrap_node(ctx.fake, "A1", ReplicasFixture.all_master(@namespace, 1))
      {:ok, tender} = start_tender(ctx, seeds: [{"10.0.0.1", 3000}])
      :ok = Tender.tend_now(tender)

      Fake.script_command(ctx.fake, "A1", {:ok, operate_reply([1], ["tags"])})

      key = Key.new(@namespace, @set, "operate-cdt")

      assert {:ok, %Record{bins: %{"tags" => 1}}} =
               Aerospike.operate(tender, key, [Op.List.size("tags")])
    end

    test "rejects unsupported operation shapes deterministically" do
      key = Key.new(@namespace, @set, "operate-bad-op")

      assert {:error, %Error{code: :invalid_argument, message: message}} =
               Operate.execute(:unused_cluster, key, [{:explode, "count", "x"}])

      assert message =~ "unsupported simple operation"
    end
  end

  defp operate_reply(values, bin_names) when is_list(values) and is_list(bin_names) do
    operations =
      Enum.zip(bin_names, values)
      |> Enum.map(fn {bin_name, value} ->
        {:ok, operation} = AsmMsg.Operation.write(bin_name, value)
        operation
      end)

    %AsmMsg{result_code: 0, operations: operations}
    |> AsmMsg.encode()
    |> IO.iodata_to_binary()
  end

  defp script_bootstrap_node(fake, node_name, replicas_value) do
    Fake.script_info(fake, node_name, ["node", "features"], %{
      "node" => node_name,
      "features" => ""
    })

    script_cycle(fake, node_name, gen: 1, peers: "0,3000,[]", replicas: replicas_value)
  end

  defp script_cycle(fake, node_name, opts) do
    Fake.script_info(fake, node_name, ["partition-generation", "cluster-stable"], %{
      "partition-generation" => Integer.to_string(Keyword.fetch!(opts, :gen)),
      "cluster-stable" => Keyword.get(opts, :cluster_stable, "deadbeef")
    })

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
