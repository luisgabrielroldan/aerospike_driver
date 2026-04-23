defmodule Aerospike.PublicApiTest do
  use ExUnit.Case, async: false

  alias Aerospike.Cluster
  alias Aerospike.Cursor
  alias Aerospike.ExecuteTask
  alias Aerospike.Filter
  alias Aerospike.Key
  alias Aerospike.PartitionFilter
  alias Aerospike.Protocol.AsmMsg
  alias Aerospike.Protocol.AsmMsg.Field
  alias Aerospike.Protocol.AsmMsg.Operation
  alias Aerospike.Protocol.Message
  alias Aerospike.Query
  alias Aerospike.Scan
  alias Aerospike.Test.ReplicasFixture
  alias Aerospike.Transport.Fake
  alias Aerospike.Txn
  alias Aerospike.Cluster.Tender

  @namespace "test"

  setup context do
    name = :"aerospike_test_#{:erlang.phash2(context.test)}"

    {:ok, fake} = Fake.start_link(nodes: [{"A1", "10.0.0.1", 3000}, {"B1", "10.0.0.2", 3000}])

    {:ok, sup} =
      Aerospike.start_link(
        name: name,
        transport: Fake,
        hosts: ["10.0.0.1:3000", "10.0.0.2:3000"],
        namespaces: [@namespace],
        connect_opts: [fake: fake],
        tend_trigger: :manual,
        pool_size: 1
      )

    script_two_node_cluster(fake)
    :ok = Tender.tend_now(name)
    assert Cluster.ready?(name)

    on_exit(fn ->
      stop_quietly(sup)
      stop_quietly(fake)
    end)

    {:ok, conn: name, conn_name: name, fake: fake, sup: sup}
  end

  test "cluster read-side helpers expose readiness and active nodes", %{conn_name: conn_name} do
    assert Cluster.ready?(conn_name)
    assert Enum.sort(Cluster.active_nodes(conn_name)) == ["A1", "B1"]
    assert Cluster.active_node?(conn_name, "A1")
    refute Cluster.active_node?(conn_name, "missing")
  end

  test "root lifecycle and key helpers expose the supported public boundary", %{
    conn_name: conn_name,
    sup: sup
  } do
    assert %{
             id: {Aerospike.Cluster.Supervisor, ^conn_name},
             start: {Aerospike.Cluster.Supervisor, :start_link, [[name: ^conn_name]]},
             type: :supervisor
           } = Aerospike.child_spec(name: conn_name)

    digest = :crypto.hash(:ripemd160, "digest-only")

    assert %Key{namespace: "test", set: "users", user_key: "user:42"} =
             Aerospike.key("test", "users", "user:42")

    assert %Key{namespace: "test", set: "users", user_key: nil, digest: ^digest} =
             Aerospike.key_digest("test", "users", digest)

    ref = Process.monitor(sup)

    assert :ok = Aerospike.close(conn_name)

    assert_receive {:DOWN, ^ref, :process, ^sup, _}, 1_000
  end

  test "single-record facade helpers normalize tuple keys and reject invalid tuples", %{
    conn: conn
  } do
    assert {:error, %Aerospike.Error{code: :invalid_argument, message: bins_message}} =
             Aerospike.get(conn, {"test", "users", "user:1"}, ["name"])

    assert bins_message =~ "supports only :all bins"

    assert {:error, %Aerospike.Error{code: :invalid_argument, message: key_message}} =
             Aerospike.exists(conn, {"test", :users, "user:1"})

    assert key_message =~ "set must be a string"
  end

  test "public scan and query wrappers return records, counts, pages, and task handles", %{
    conn: conn,
    fake: fake
  } do
    scan = Scan.new(@namespace, "scan_ops")

    Fake.script_stream(fake, "A1", {:ok, [frame("stream-A1"), last_frame()]})
    Fake.script_stream(fake, "B1", {:ok, [frame("stream-B1"), last_frame()]})

    assert {:ok, stream} = Aerospike.scan_stream(conn, scan)
    assert Enum.sort(Enum.map(stream, & &1.bins["payload"])) == ["stream-A1", "stream-B1"]

    Fake.script_stream(fake, "A1", {:ok, [frame("all-A1"), last_frame()]})
    Fake.script_stream(fake, "B1", {:ok, [frame("all-B1"), last_frame()]})

    assert {:ok, records} = Aerospike.scan_all(conn, scan)
    assert Enum.sort(Enum.map(records, & &1.bins["payload"])) == ["all-A1", "all-B1"]

    Fake.script_stream(fake, "A1", {:ok, [frame("count-A1"), frame("count-A2"), last_frame()]})
    Fake.script_stream(fake, "B1", {:ok, [frame("count-B1"), last_frame()]})

    assert 3 = Aerospike.scan_count!(conn, scan)

    Fake.script_stream(fake, "A1", {:ok, [frame("node-A1"), last_frame()]})
    assert [%{bins: %{"payload" => "node-A1"}}] = Aerospike.scan_all!(conn, scan, node: "A1")

    Fake.script_stream(fake, "A1", {:ok, [frame("node-count"), last_frame()]})
    assert 1 = Aerospike.scan_count!(conn, scan, node: "A1")

    query =
      Query.new(@namespace, "scan_ops")
      |> Query.where(Filter.range("payload", 0, 9))
      |> Query.max_records(1)

    Fake.script_stream(fake, "A1", {:ok, [frame("q-count-A1"), last_frame()]})

    Fake.script_stream(
      fake,
      "B1",
      {:ok, [frame("q-count-B1"), frame("q-count-B2"), last_frame()]}
    )

    assert 3 = Aerospike.query_count!(conn, query)

    Fake.script_stream(
      fake,
      "A1",
      {:ok, [frame("page-1"), partition_done_frame("page-1"), last_frame()]}
    )

    Fake.script_stream(fake, "B1", {:ok, [last_frame()]})

    assert %{records: [%{bins: %{"payload" => "page-1"}}], done?: false, cursor: %Cursor{}} =
             Aerospike.query_page!(conn, query)

    Fake.script_stream(fake, "A1", {:ok, [frame("page-node-1"), last_frame()]})

    node_query = Query.partition_filter(query, PartitionFilter.by_id(0))

    assert %{records: [%{bins: %{"payload" => "page-node-1"}}], done?: false, cursor: %Cursor{}} =
             Aerospike.query_page!(conn, node_query, node: "A1")

    Fake.script_stream(fake, "A1", {:ok, [frame("page-node-count"), last_frame()]})
    assert 1 = Aerospike.query_count!(conn, node_query, node: "A1")

    Fake.script_command(fake, "A1", {:ok, scripted_reply_body(0, 4, 60)})

    assert {:ok, %ExecuteTask{kind: :query_execute}} =
             Aerospike.query_execute(conn, query, [], node: "A1")

    Fake.script_command(fake, "A1", {:ok, scripted_reply_body(0, 4, 60)})

    assert {:ok, %ExecuteTask{kind: :query_udf}} =
             Aerospike.query_udf(conn, query, "pkg", "fun", [], node: "A1")
  end

  test "bare scan aliases stay compatible with the explicit scan helpers", %{
    conn: conn,
    fake: fake
  } do
    scan = Scan.new(@namespace, "scan_ops")

    Fake.script_stream(fake, "A1", {:ok, [frame("alias-stream"), last_frame()]})

    assert {:ok, stream} = Aerospike.scan_stream(conn, scan, node: "A1")
    assert ["alias-stream"] = Enum.map(stream, & &1.bins["payload"])

    Fake.script_stream(fake, "A1", {:ok, [frame("alias-stream"), last_frame()]})

    alias_stream =
      apply(Aerospike, :stream!, [conn, scan, [node: "A1"]])
      |> Enum.map(& &1.bins["payload"])

    assert ["alias-stream"] = alias_stream

    Fake.script_stream(fake, "A1", {:ok, [frame("alias-all"), last_frame()]})

    assert {:ok, [%{bins: %{"payload" => "alias-all"}}]} =
             Aerospike.scan_all(conn, scan, node: "A1")

    Fake.script_stream(fake, "A1", {:ok, [frame("alias-all"), last_frame()]})

    assert {:ok, [%{bins: %{"payload" => "alias-all"}}]} =
             apply(Aerospike, :all, [conn, scan, [node: "A1"]])

    Fake.script_stream(fake, "A1", {:ok, [frame("alias-count"), last_frame()]})
    assert 1 = Aerospike.scan_count!(conn, scan, node: "A1")

    Fake.script_stream(fake, "A1", {:ok, [frame("alias-count"), last_frame()]})
    assert 1 = apply(Aerospike, :count!, [conn, scan, [node: "A1"]])
  end

  test "bang wrappers raise the underlying public errors", %{conn: conn} do
    scan = Scan.new(@namespace, "scan_ops")
    query = Query.new(@namespace, "scan_ops") |> Query.where(Filter.range("payload", 0, 9))

    assert_raise Aerospike.Error, fn ->
      Aerospike.scan_stream!(conn, scan, node: "missing") |> Enum.to_list()
    end

    assert_raise Aerospike.Error, fn ->
      apply(Aerospike, :stream!, [conn, scan, [node: "missing"]]) |> Enum.to_list()
    end

    assert_raise Aerospike.Error, ~r/max_records_required/i, fn ->
      Aerospike.query_all!(conn, query)
    end

    assert_raise Aerospike.Error, ~r/invalid cursor/i, fn ->
      Aerospike.query_page!(conn, Query.max_records(query, 1), cursor: 123)
    end
  end

  test "public query wrappers cover node streams, resumable pages, and background jobs", %{
    conn: conn,
    fake: fake
  } do
    query =
      Query.new(@namespace, "scan_ops")
      |> Query.where(Filter.range("payload", 0, 9))
      |> Query.max_records(1)

    Fake.script_stream(fake, "A1", {:ok, [frame("stream-node"), last_frame()]})

    assert [%{bins: %{"payload" => "stream-node"}}] =
             conn
             |> Aerospike.query_stream!(query, node: "A1")
             |> Enum.to_list()

    Fake.script_stream(
      fake,
      "A1",
      {:ok, [frame("page-1"), partition_done_frame("page-1"), last_frame()]}
    )

    Fake.script_stream(fake, "B1", {:ok, [last_frame()]})

    assert %{records: [%{bins: %{"payload" => "page-1"}}], cursor: %Cursor{} = cursor} =
             Aerospike.query_page!(conn, query)

    Fake.script_stream(
      fake,
      "A1",
      {:ok, [frame("page-2"), partition_done_frame("page-2"), last_frame()]}
    )

    Fake.script_stream(fake, "B1", {:ok, [last_frame()]})

    assert %{records: [%{bins: %{"payload" => "page-2"}}], cursor: %Cursor{}} =
             Aerospike.query_page!(conn, query, cursor: cursor)

    Fake.script_command(fake, "A1", {:ok, scripted_reply_body(0, 4, 60)})
    Fake.script_command(fake, "B1", {:ok, scripted_reply_body(0, 4, 60)})

    assert {:ok, %ExecuteTask{kind: :query_execute, node_name: nil}} =
             Aerospike.query_execute(conn, query, [])

    Fake.script_command(fake, "A1", {:ok, scripted_reply_body(0, 4, 60)})
    Fake.script_command(fake, "B1", {:ok, scripted_reply_body(0, 4, 60)})

    assert {:ok, %ExecuteTask{kind: :query_udf, node_name: nil}} =
             Aerospike.query_udf(conn, query, "pkg", "fun", [])
  end

  test "explicit node-targeted helpers cover scan and query bang paths", %{
    conn: conn,
    fake: fake
  } do
    scan = Scan.new(@namespace, "scan_ops")

    query =
      Query.new(@namespace, "scan_ops")
      |> Query.where(Filter.range("payload", 0, 9))
      |> Query.max_records(1)

    Fake.script_stream(fake, "A1", {:ok, [frame("node-scan"), last_frame()]})

    assert [%{bins: %{"payload" => "node-scan"}}] =
             Aerospike.scan_all!(conn, scan, node: "A1")

    Fake.script_stream(fake, "A1", {:ok, [frame("node-query"), last_frame()]})

    assert [%{bins: %{"payload" => "node-query"}}] =
             conn
             |> Aerospike.query_stream!(query, node: "A1")
             |> Enum.to_list()

    Fake.script_command(fake, "A1", {:ok, scripted_reply_body(0, 4, 60)})

    assert {:ok, %ExecuteTask{kind: :query_execute, node_name: "A1"}} =
             Aerospike.query_execute(conn, query, [], node: "A1")
  end

  test "transaction wrappers initialize tracking, abort on explicit errors, and reject reused handles",
       %{
         conn_name: conn_name
       } do
    assert {:ok, :done} =
             Aerospike.transaction(conn_name, fn txn ->
               assert {:ok, :open} = Aerospike.txn_status(conn_name, txn)
               :done
             end)

    txn = Txn.new()

    assert {:error, %Aerospike.Error{code: :timeout}} =
             Aerospike.transaction(conn_name, txn, fn tx ->
               assert {:ok, :open} = Aerospike.txn_status(conn_name, tx)
               raise Aerospike.Error.from_result_code(:timeout)
             end)

    assert {:error, %Aerospike.Error{code: :parameter_error}} = Aerospike.commit(conn_name, txn)
    assert {:error, %Aerospike.Error{code: :parameter_error}} = Aerospike.abort(conn_name, txn)
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
      "replicas" => ReplicasFixture.build(@namespace, 1, [Enum.to_list(0..100), []])
    })

    Fake.script_info(fake, "B1", ["replicas"], %{
      "replicas" => ReplicasFixture.build(@namespace, 1, [Enum.to_list(101..4095), []])
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
      fields: [Field.digest(digest_fixture(payload))],
      operations: []
    }
  end

  defp encode_bin(msg), do: IO.iodata_to_binary(Message.encode_as_msg_iodata(AsmMsg.encode(msg)))
  defp digest_fixture(seed), do: :crypto.hash(:ripemd160, seed)

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
