defmodule Aerospike.Command.ScanOps.StreamRunnerTest do
  use ExUnit.Case, async: true

  alias Aerospike.Cluster.NodeSupervisor
  alias Aerospike.Cluster.PartitionMapWriter
  alias Aerospike.Cluster.TableOwner
  alias Aerospike.Cluster.Tender
  alias Aerospike.Command.ScanOps.StreamRunner
  alias Aerospike.Filter
  alias Aerospike.Protocol.AsmMsg
  alias Aerospike.Protocol.AsmMsg.Field
  alias Aerospike.Protocol.AsmMsg.Operation
  alias Aerospike.Protocol.Message
  alias Aerospike.Query
  alias Aerospike.Scan
  alias Aerospike.Test.ReplicasFixture
  alias Aerospike.Transport.Fake

  @namespace "test"

  setup context do
    name = :"stream_runner_test_#{:erlang.phash2(context.test)}"

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

    %{name: name, fake: fake, tables: tables, node_sup_name: NodeSupervisor.sup_name(name)}
  end

  test "stream_node returns records from a node-scoped scan", ctx do
    script_one_node_cluster(ctx.fake)
    {:ok, tender} = start_tender(ctx)
    :ok = Tender.tend_now(tender)

    Fake.script_stream(ctx.fake, "A1", {:ok, [frame("streamed"), last_frame()]})

    assert {:ok, stream} =
             StreamRunner.stream_node(tender, "A1", Scan.new(@namespace, "scan_ops"), [])

    assert [%{bins: %{"payload" => "streamed"}}] = Enum.to_list(stream)
  end

  test "query_aggregate streams aggregate success payloads" do
    ctx = setup_aggregate_cluster!()

    Fake.script_stream(ctx.fake, "A1", {:ok, [aggregate_frame("agg-result"), last_frame()]})

    query =
      Query.new(@namespace, "scan_ops")
      |> Query.where(Filter.range("payload", 0, 9))

    assert {:ok, stream} = StreamRunner.query_aggregate(ctx.tender, query, [], "pkg", "fn", [1])
    assert Enum.to_list(stream) == ["agg-result"]
  end

  test "query_execute_node validates node filters before dispatch", ctx do
    script_one_node_cluster(ctx.fake)
    {:ok, tender} = start_tender(ctx)
    :ok = Tender.tend_now(tender)

    query =
      Query.new(@namespace, "scan_ops")
      |> Query.where(Filter.range("payload", 0, 9))

    assert {:error, %Aerospike.Error{code: :invalid_node, message: message}} =
             StreamRunner.query_execute_node(tender, "missing", query, [], [])

    assert message =~ "query target node unavailable: missing"
  end

  test "query_execute and query_udf return background task handles", ctx do
    script_one_node_cluster(ctx.fake)
    {:ok, tender} = start_tender(ctx)
    :ok = Tender.tend_now(tender)

    query =
      Query.new(@namespace, "scan_ops")
      |> Query.where(Filter.range("payload", 0, 9))

    Fake.script_command(ctx.fake, "A1", {:ok, metadata_body()})

    assert {:ok, %Aerospike.ExecuteTask{kind: :query_execute, node_name: nil}} =
             StreamRunner.query_execute(tender, query, [], max_concurrent_nodes: 2)

    Fake.script_command(ctx.fake, "A1", {:ok, metadata_body()})

    assert {:ok, %Aerospike.ExecuteTask{kind: :query_execute, node_name: nil}} =
             StreamRunner.query_execute(tender, query, [])

    Fake.script_command(ctx.fake, "A1", {:ok, metadata_body()})

    assert {:ok, %Aerospike.ExecuteTask{kind: :query_udf, node_name: nil}} =
             StreamRunner.query_udf(tender, query, "pkg", "fn", [])

    Fake.script_command(ctx.fake, "A1", {:ok, metadata_body()})

    assert {:ok, %Aerospike.ExecuteTask{kind: :query_udf, node_name: "A1"}} =
             StreamRunner.query_udf_node(tender, "A1", query, "pkg", "fn", [], [])

    assert {:error, %Aerospike.Error{code: :invalid_node}} =
             StreamRunner.query_execute_node(tender, "missing", query, [])

    assert {:error, %Aerospike.Error{code: :invalid_node}} =
             StreamRunner.query_udf_node(tender, "missing", query, "pkg", "fn", [])
  end

  defp setup_aggregate_cluster! do
    name = :"stream_runner_aggregate_#{System.unique_integer([:positive])}"

    {:ok, fake} = Fake.start_link(nodes: [{"A1", "10.0.0.1", 3000}])
    {:ok, owner} = TableOwner.start_link(name: name)
    tables = TableOwner.tables(owner)
    {:ok, writer} = PartitionMapWriter.start_link(name: name, tables: tables)
    {:ok, node_sup} = NodeSupervisor.start_link(name: name)

    ctx = %{name: name, fake: fake, tables: tables, node_sup_name: NodeSupervisor.sup_name(name)}
    script_one_node_cluster(fake)
    {:ok, tender} = start_tender(ctx)
    :ok = Tender.tend_now(tender)

    on_exit(fn ->
      stop_quietly(tender)
      stop_quietly(node_sup)
      stop_quietly(writer)
      stop_quietly(owner)
      stop_quietly(fake)
    end)

    Map.put(ctx, :tender, tender)
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

  defp script_one_node_cluster(fake) do
    Fake.script_info(fake, "A1", ["node", "features"], %{"node" => "A1", "features" => ""})

    Fake.script_info(
      fake,
      "A1",
      ["partition-generation", "cluster-stable", "peers-generation"],
      %{
        "partition-generation" => "1",
        "cluster-stable" => "deadbeef",
        "peers-generation" => "1"
      }
    )

    Fake.script_info(fake, "A1", ["peers-clear-std"], %{"peers-clear-std" => "0,3000,[]"})

    Fake.script_info(fake, "A1", ["replicas"], %{
      "replicas" => ReplicasFixture.build(@namespace, 1, [Enum.to_list(0..4095), []])
    })
  end

  defp frame(payload), do: {:frame, encode_bin(record_msg(payload))}
  defp aggregate_frame(payload), do: {:frame, encode_bin(aggregate_msg(payload))}
  defp last_frame, do: {:frame, encode_bin(%AsmMsg{info3: AsmMsg.info3_last()})}
  defp metadata_body, do: <<22, 0, 0, 0, 0, 0, 1::32-big, 120::32-big, 0::32, 0::16, 0::16>>

  defp record_msg(payload) do
    %AsmMsg{
      info1: AsmMsg.info1_read(),
      result_code: 0,
      generation: 7,
      expiration: 120,
      fields: [
        Field.namespace(@namespace),
        Field.set("scan_ops"),
        Field.digest(digest_fixture(payload))
      ],
      operations: [
        %Operation{
          op_type: Operation.op_read(),
          particle_type: 3,
          bin_name: "payload",
          data: payload
        }
      ]
    }
  end

  defp aggregate_msg(payload) do
    %AsmMsg{
      info1: AsmMsg.info1_read(),
      result_code: 0,
      fields: [],
      operations: [
        %Operation{
          op_type: Operation.op_read(),
          particle_type: 3,
          bin_name: "SUCCESS",
          data: payload
        }
      ]
    }
  end

  defp encode_bin(msg), do: IO.iodata_to_binary(Message.encode_as_msg_iodata(AsmMsg.encode(msg)))
  defp digest_fixture(seed), do: :crypto.hash(:ripemd160, seed)

  defp stop_quietly(pid) when is_pid(pid) do
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
