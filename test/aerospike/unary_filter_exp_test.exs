defmodule Aerospike.UnaryFilterExpTest do
  use ExUnit.Case, async: true

  alias Aerospike.Command.ApplyUdf
  alias Aerospike.Command.Delete
  alias Aerospike.Command.Exists
  alias Aerospike.Command.Get
  alias Aerospike.Command.Operate
  alias Aerospike.Command.Put
  alias Aerospike.Command.Touch
  alias Aerospike.Command.WriteOp
  alias Aerospike.Cluster.NodeSupervisor
  alias Aerospike.Cluster.PartitionMapWriter
  alias Aerospike.Cluster.TableOwner
  alias Aerospike.Cluster.Tender
  alias Aerospike.Exp
  alias Aerospike.Key
  alias Aerospike.Protocol.AsmMsg
  alias Aerospike.Protocol.AsmMsg.Field
  alias Aerospike.Protocol.Message
  alias Aerospike.Test.ReplicasFixture
  alias Aerospike.Transport.Fake

  @namespace "test"
  @set "spike"
  @filter_wire <<0xC3>>

  setup context do
    name = :"unary_filter_exp_#{:erlang.phash2(context.test)}"

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
      fake: fake,
      name: name,
      node_sup_name: NodeSupervisor.sup_name(name),
      tables: tables
    }
  end

  test "attaches one expression filter field to supported unary requests", ctx do
    script_bootstrap_node(ctx.fake)
    {:ok, tender} = start_tender(ctx)
    :ok = Tender.tend_now(tender)

    filter = Exp.from_wire(@filter_wire)

    cases = [
      {"get", metadata_reply(2), fn key -> Get.execute(tender, key, :all, filter: filter) end},
      {"exists", metadata_reply(2), fn key -> Exists.execute(tender, key, filter: filter) end},
      {"put", metadata_reply(0),
       fn key -> Put.execute(tender, key, %{count: 1}, filter: filter) end},
      {"delete", metadata_reply(0), fn key -> Delete.execute(tender, key, filter: filter) end},
      {"touch", metadata_reply(0), fn key -> Touch.execute(tender, key, filter: filter) end},
      {"write_op", metadata_reply(0),
       fn key -> WriteOp.execute(tender, key, :add, %{count: 1}, filter: filter) end},
      {"operate", operate_reply([], []),
       fn key -> Operate.execute(tender, key, [{:write, "count", 1}], filter: filter) end},
      {"apply_udf", udf_reply("SUCCESS", "ok"),
       fn key -> ApplyUdf.execute(tender, key, "pkg", "fun", [], filter: filter) end}
    ]

    for {name, reply, execute} <- cases do
      Fake.script_command(ctx.fake, "A1", {:ok, reply})

      _result = execute.(key(name))

      msg = last_command_msg(ctx.fake)
      assert [%Field{data: @filter_wire}] = filter_fields(msg)
    end
  end

  test "omits the expression filter field when no filter is provided", ctx do
    script_bootstrap_node(ctx.fake)
    {:ok, tender} = start_tender(ctx)
    :ok = Tender.tend_now(tender)

    Fake.script_command(ctx.fake, "A1", {:ok, metadata_reply(2)})

    _result = Get.execute(tender, key("no-filter"), :all)

    assert [] = last_command_msg(ctx.fake) |> filter_fields()
  end

  defp last_command_msg(fake) do
    request = Fake.last_command_request(fake, "A1")
    assert {:ok, {_version, type, body}} = request |> IO.iodata_to_binary() |> Message.decode()
    assert type == Message.type_as_msg()
    assert {:ok, msg} = AsmMsg.decode(body)
    msg
  end

  defp filter_fields(%AsmMsg{fields: fields}) do
    Enum.filter(fields, &(&1.type == Field.type_filter_exp()))
  end

  defp key(suffix), do: Key.new(@namespace, @set, "filter-#{suffix}")

  defp metadata_reply(result_code) do
    <<22, 0, 0, 0, 0, result_code::8, 0::32-big, 0::32-big, 0::32-big, 0::16, 0::16>>
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

  defp udf_reply(bin_name, value) do
    {:ok, operation} = AsmMsg.Operation.write(bin_name, value)

    %AsmMsg{operations: [operation]}
    |> AsmMsg.encode()
    |> IO.iodata_to_binary()
  end

  defp script_bootstrap_node(fake) do
    Fake.script_info(fake, "A1", ["node", "features"], %{
      "node" => "A1",
      "features" => ""
    })

    script_cycle(fake,
      gen: 1,
      peers: "0,3000,[]",
      replicas: ReplicasFixture.all_master(@namespace, 1)
    )
  end

  defp script_cycle(fake, opts) do
    Fake.script_info(
      fake,
      "A1",
      ["partition-generation", "cluster-stable", "peers-generation"],
      %{
        "partition-generation" => Integer.to_string(Keyword.fetch!(opts, :gen)),
        "cluster-stable" => "deadbeef",
        "peers-generation" => "1"
      }
    )

    Fake.script_info(fake, "A1", ["peers-clear-std"], %{
      "peers-clear-std" => Keyword.fetch!(opts, :peers)
    })

    Fake.script_info(fake, "A1", ["replicas"], %{
      "replicas" => Keyword.fetch!(opts, :replicas)
    })
  end

  defp start_tender(ctx) do
    {:ok, pid} =
      Tender.start_link(
        name: ctx.name,
        transport: Fake,
        connect_opts: [fake: ctx.fake],
        seeds: [{"10.0.0.1", 3000}],
        namespaces: [@namespace],
        tables: ctx.tables,
        tend_trigger: :manual,
        node_supervisor: ctx.node_sup_name,
        pool_size: 1
      )

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
