defmodule Aerospike.ScanOpsTest do
  use ExUnit.Case, async: true

  alias Aerospike
  alias Aerospike.Cursor
  alias Aerospike.Filter
  alias Aerospike.Key
  alias Aerospike.NodePartitions
  alias Aerospike.NodeSupervisor
  alias Aerospike.PartitionFilter
  alias Aerospike.PartitionMapWriter
  alias Aerospike.Protocol.AsmMsg
  alias Aerospike.Protocol.AsmMsg.Field
  alias Aerospike.Protocol.AsmMsg.Operation
  alias Aerospike.Protocol.Message
  alias Aerospike.Query
  alias Aerospike.Record
  alias Aerospike.Scan
  alias Aerospike.TableOwner
  alias Aerospike.Tender
  alias Aerospike.Test.ReplicasFixture
  alias Aerospike.Transport.Fake

  @namespace "test"

  setup context do
    name = :"scan_ops_test_#{:erlang.phash2(context.test)}"

    {:ok, fake} = Fake.start_link(nodes: [{"A1", "10.0.0.1", 3000}, {"B1", "10.0.0.2", 3000}])
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

  test "scan fan-out streams records from multiple nodes", ctx do
    script_two_node_cluster(ctx.fake)
    {:ok, tender} = start_tender(ctx)
    :ok = Tender.tend_now(tender)

    scan = Scan.new(@namespace, "scan_ops")

    Fake.script_stream(ctx.fake, "A1", {:ok, [frame("A1-1"), frame("A1-2"), last_frame()]})
    Fake.script_stream(ctx.fake, "B1", {:ok, [frame("B1-1"), last_frame()]})

    stream_payloads =
      tender
      |> Aerospike.stream!(scan)
      |> Enum.map(& &1.bins["payload"])
      |> Enum.sort()

    assert stream_payloads == ["A1-1", "A1-2", "B1-1"]
  end

  test "node-targeted scan helpers stay scoped to the requested node", ctx do
    script_two_node_cluster(ctx.fake)
    {:ok, tender} = start_tender(ctx)
    :ok = Tender.tend_now(tender)

    scan = Scan.new(@namespace, "scan_ops")

    Fake.script_stream(ctx.fake, "A1", {:ok, [frame("A1-only"), last_frame()]})

    assert {:ok, [record]} = Aerospike.scan_all_node(tender, "A1", scan)
    assert record.bins["payload"] == "A1-only"
  end

  test "query helpers expose a lazy outer stream and resumable collected pages", ctx do
    script_two_node_cluster(ctx.fake)
    {:ok, tender} = start_tender(ctx)
    :ok = Tender.tend_now(tender)

    query =
      Query.new(@namespace, "scan_ops")
      |> Query.where(Filter.range("payload", 0, 9))

    Fake.script_stream(ctx.fake, "A1", {:ok, [frame("Q-A1-1"), frame("Q-A1-2"), last_frame()]})
    Fake.script_stream(ctx.fake, "B1", {:ok, [frame("Q-B1-1"), last_frame()]})

    stream_payloads =
      tender
      |> Aerospike.query_stream!(query)
      |> Enum.map(& &1.bins["payload"])
      |> Enum.sort()

    assert stream_payloads == ["Q-A1-1", "Q-A1-2", "Q-B1-1"]

    paged_query =
      query
      |> Query.partition_filter(PartitionFilter.by_id(0))
      |> Query.max_records(1)

    Fake.script_stream(
      ctx.fake,
      "A1",
      {:ok, [frame("page-1"), partition_done_frame("page-1"), last_frame()]}
    )

    Fake.script_stream(
      ctx.fake,
      "A1",
      {:ok, [frame("page-3"), partition_done_frame("page-3"), last_frame()]}
    )

    assert {:ok, page1} = Aerospike.query_page_node(tender, "A1", paged_query)
    assert [%{bins: %{"payload" => "page-1"}}] = page1.records
    assert page1.done? == false
    assert %Cursor{} = page1.cursor

    encoded_cursor = Cursor.encode(page1.cursor)

    assert {:ok, page2} =
             Aerospike.query_page_node(tender, "A1", paged_query, cursor: encoded_cursor)

    assert [%{bins: %{"payload" => "page-3"}}] = page2.records
    assert %Cursor{} = page2.cursor
  end

  test "query collection helpers require an explicit max_records budget", ctx do
    script_two_node_cluster(ctx.fake)
    {:ok, tender} = start_tender(ctx)
    :ok = Tender.tend_now(tender)

    query =
      Query.new(@namespace, "scan_ops")
      |> Query.where(Filter.range("payload", 0, 9))

    assert {:error, %Aerospike.Error{code: :max_records_required}} =
             Aerospike.query_all(tender, query)

    assert {:error, %Aerospike.Error{code: :max_records_required}} =
             Aerospike.query_page(tender, query)
  end

  test "stream, page, and background query helpers share node-targeting validation", ctx do
    script_two_node_cluster(ctx.fake)
    {:ok, tender} = start_tender(ctx)
    :ok = Tender.tend_now(tender)

    query =
      Query.new(@namespace, "scan_ops")
      |> Query.where(Filter.range("payload", 0, 9))
      |> Query.max_records(1)

    assert {:error, %Aerospike.Error{code: :invalid_node}} =
             Aerospike.query_stream_node(tender, "missing", query)

    assert {:error, %Aerospike.Error{code: :invalid_node}} =
             Aerospike.query_page_node(tender, "missing", query)

    assert {:error, %Aerospike.Error{code: :invalid_node}} =
             Aerospike.query_execute_node(tender, "missing", query, [])
  end

  test "background query execution reuses the shared node preparation seam", ctx do
    script_two_node_cluster(ctx.fake)
    {:ok, tender} = start_tender(ctx)
    :ok = Tender.tend_now(tender)

    query =
      Query.new(@namespace, "scan_ops")
      |> Query.where(Filter.range("payload", 0, 9))

    Fake.script_command(ctx.fake, "A1", {:ok, scripted_reply_body(0, 4, 60)})
    Fake.script_command(ctx.fake, "B1", {:ok, scripted_reply_body(0, 4, 60)})

    assert {:ok,
            %Aerospike.ExecuteTask{
              kind: :query_execute,
              namespace: @namespace,
              set: "scan_ops"
            }} = Aerospike.query_execute(tender, query, [])
  end

  test "count and paging surface deterministic parse and cursor errors as Aerospike.Error values",
       ctx do
    script_two_node_cluster(ctx.fake)
    {:ok, tender} = start_tender(ctx)
    :ok = Tender.tend_now(tender)

    scan = Scan.new(@namespace, "scan_ops")

    Fake.script_stream(ctx.fake, "A1", {:ok, [frame("ok"), {:frame, <<0, 1, 2>>}]})
    Fake.script_stream(ctx.fake, "B1", {:ok, [last_frame()]})

    assert {:error, %Aerospike.Error{code: :parse_error}} = Aerospike.ScanOps.count(tender, scan)

    query =
      Query.new(@namespace, "scan_ops")
      |> Query.where(Filter.range("payload", 0, 9))
      |> Query.max_records(1)

    assert {:error, %Aerospike.Error{code: :parameter_error, message: message}} =
             Aerospike.ScanOps.query_page(tender, query, cursor: 123)

    assert message =~ "invalid cursor"

    assert {:error, %Aerospike.Error{code: :parse_error}} =
             Aerospike.ScanOps.query_page(tender, query, cursor: "not-base64")
  end

  test "allow_record_fold enforces the tracker record budget without reordering kept records" do
    tracker = Aerospike.PartitionTracker.new(PartitionFilter.all(), nodes: ["A1"], max_records: 1)
    tracker = %{tracker | record_count: 0}
    node_partitions = NodePartitions.new("A1")

    records = [
      %Record{
        key: Key.new(@namespace, "scan_ops", "keep"),
        bins: %{"payload" => "keep"},
        generation: 1,
        ttl: 60
      },
      %Record{
        key: Key.new(@namespace, "scan_ops", "drop"),
        bins: %{"payload" => "drop"},
        generation: 1,
        ttl: 60
      }
    ]

    {tracker2, node_partitions2, kept_records} =
      Aerospike.ScanOps.allow_record_fold(tracker, node_partitions, records)

    assert tracker2.record_count == 2
    assert node_partitions2.disallowed_count == 1
    assert Enum.map(kept_records, & &1.bins["payload"]) == ["keep"]
  end

  defp start_tender(ctx) do
    {:ok, pid} =
      Tender.start_link(
        name: ctx.name,
        transport: Fake,
        connect_opts: [fake: ctx.fake],
        seeds: [{"10.0.0.1", 3000}, {"10.0.0.2", 3000}],
        namespaces: ["test"],
        tables: ctx.tables,
        tend_trigger: :manual,
        node_supervisor: ctx.node_sup_name,
        pool_size: 1
      )

    on_exit(fn -> stop_quietly(pid) end)
    {:ok, pid}
  end

  defp script_two_node_cluster(fake) do
    Fake.script_info(fake, "A1", ["node", "features"], %{"node" => "A1", "features" => ""})
    Fake.script_info(fake, "B1", ["node", "features"], %{"node" => "B1", "features" => ""})

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

    Fake.script_info(
      fake,
      "B1",
      ["partition-generation", "cluster-stable", "peers-generation"],
      %{
        "partition-generation" => "1",
        "cluster-stable" => "deadbeef",
        "peers-generation" => "1"
      }
    )

    Fake.script_info(fake, "A1", ["peers-clear-std"], %{"peers-clear-std" => "0,3000,[]"})
    Fake.script_info(fake, "B1", ["peers-clear-std"], %{"peers-clear-std" => "0,3000,[]"})

    Fake.script_info(fake, "A1", ["replicas"], %{
      "replicas" => ReplicasFixture.build("test", 1, [Enum.to_list(0..100), []])
    })

    Fake.script_info(fake, "B1", ["replicas"], %{
      "replicas" => ReplicasFixture.build("test", 1, [Enum.to_list(101..4095), []])
    })
  end

  defp frame(payload) do
    {:frame, encode_bin(record_msg(payload))}
  end

  defp partition_done_frame(payload) do
    {:frame, encode_bin(partition_done_msg(payload))}
  end

  defp last_frame do
    {:frame, encode_bin(%AsmMsg{info3: AsmMsg.info3_last()})}
  end

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

  defp partition_done_msg(payload) do
    %AsmMsg{
      info3: 0x04,
      result_code: 0,
      generation: 0,
      expiration: 0,
      fields: [
        Field.digest(digest_fixture(payload))
      ],
      operations: []
    }
  end

  defp encode_bin(msg), do: IO.iodata_to_binary(Message.encode_as_msg_iodata(AsmMsg.encode(msg)))

  defp digest_fixture(seed) do
    :crypto.hash(:ripemd160, seed)
  end

  defp scripted_reply_body(result_code, generation, ttl) do
    <<22, 0, 0, 0, 0, result_code::8, generation::32-big, ttl::32-big, 0::32, 0::16, 0::16>>
  end

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
