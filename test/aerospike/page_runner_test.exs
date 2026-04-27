defmodule Aerospike.Command.ScanOps.PageRunnerTest do
  use ExUnit.Case, async: true

  alias Aerospike.Cluster.NodeSupervisor
  alias Aerospike.Cluster.PartitionMapWriter
  alias Aerospike.Cluster.TableOwner
  alias Aerospike.Cluster.Tender
  alias Aerospike.Command.ScanOps.PageRunner
  alias Aerospike.Cursor
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
    name = :"page_runner_test_#{:erlang.phash2(context.test)}"

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

  test "runtime rejects scans until the tender is ready", ctx do
    {:ok, tender} = start_tender(ctx)

    assert {:error, %Aerospike.Error{code: :cluster_not_ready, message: message}} =
             PageRunner.runtime(tender, Scan.new(@namespace, "scan_ops"))

    assert message =~ "scan requires a ready cluster"
  end

  test "prepare_node_requests activates the record budget and carries validated policy", ctx do
    script_two_node_cluster(ctx.fake)
    {:ok, tender} = start_tender(ctx)
    :ok = Tender.tend_now(tender)

    query =
      Query.new(@namespace, "scan_ops")
      |> Query.where(Filter.range("payload", 0, 9))
      |> Query.max_records(5)

    assert {:ok, runtime} = PageRunner.runtime(tender, query)

    assert {:ok, tracker, node_requests} =
             PageRunner.prepare_node_requests(runtime, query, nil,
               timeout: 1_234,
               socket_timeout: 250,
               max_retries: 3,
               sleep_between_retries_ms: 15,
               replica_policy: :master,
               task_id: 77,
               pool_checkout_timeout: 99
             )

    assert tracker.record_count == 0
    assert tracker.socket_timeout == 250
    assert tracker.total_timeout == 1_234
    assert tracker.max_retries == 3
    assert tracker.sleep_between_retries == 15
    assert tracker.replica == :master
    assert Enum.sort(Enum.map(node_requests, & &1.node_name)) == ["A1", "B1"]

    assert Enum.all?(node_requests, fn request ->
             request.policy.timeout == 1_234 and
               request.policy.socket_timeout == 250 and
               request.policy.task_id == 77 and
               request.pool_checkout_timeout == 99 and
               match?(%Query{}, request.scannable)
           end)
  end

  test "prepare_node_requests rejects unknown node filters and missing namespaces", ctx do
    script_two_node_cluster(ctx.fake)
    {:ok, tender} = start_tender(ctx)
    :ok = Tender.tend_now(tender)

    query =
      Query.new(@namespace, "scan_ops")
      |> Query.where(Filter.range("payload", 0, 9))
      |> Query.max_records(1)

    assert {:ok, runtime} = PageRunner.runtime(tender, query)

    assert {:error, %Aerospike.Error{code: :invalid_node}} =
             PageRunner.prepare_node_requests(runtime, query, "missing", [])

    missing_ns_query =
      Query.new("missing", "scan_ops")
      |> Query.where(Filter.range("payload", 0, 9))
      |> Query.max_records(1)

    assert {:error, %Aerospike.Error{code: :cluster_not_ready, message: message}} =
             PageRunner.prepare_node_requests(runtime, missing_ns_query, nil, [])

    assert message =~ "query requires a ready cluster"
  end

  test "prepare_node_requests supports node-scoped scans and rejects invalid runtime opts", ctx do
    script_two_node_cluster(ctx.fake)
    {:ok, tender} = start_tender(ctx)
    :ok = Tender.tend_now(tender)

    scan = Scan.new(@namespace, "scan_ops") |> Scan.max_records(2)

    assert {:ok, runtime} = PageRunner.runtime(tender, scan)

    assert {:ok, tracker, [%{node_name: "A1"} = request]} =
             PageRunner.prepare_node_requests(runtime, scan, "A1", [])

    assert tracker.node_filter == "A1"
    assert match?(%Scan{}, request.scannable)

    assert {:error, %Aerospike.Error{code: :invalid_argument}} =
             PageRunner.prepare_node_requests(runtime, scan, "A1", timeout: -1)
  end

  test "prepare_node_requests rejects cluster snapshots with no active nodes", ctx do
    runtime = %{tender: ctx.name, transport: Fake, tables: ctx.tables}

    assert {:error, %Aerospike.Error{code: :cluster_not_ready, message: message}} =
             PageRunner.prepare_node_requests(runtime, Scan.new(@namespace, "scan_ops"), nil, [])

    assert message =~ "scan requires active nodes"
  end

  test "page accepts a Cursor struct and surfaces unexpected frame types as parse errors", ctx do
    script_two_node_cluster(ctx.fake)
    {:ok, tender} = start_tender(ctx)
    :ok = Tender.tend_now(tender)

    query =
      Query.new(@namespace, "scan_ops")
      |> Query.where(Filter.range("payload", 0, 9))
      |> Query.max_records(1)

    Fake.script_stream(
      ctx.fake,
      "A1",
      {:ok, [frame("page-1"), partition_done_frame("page-1"), last_frame()]}
    )

    Fake.script_stream(ctx.fake, "B1", {:ok, [last_frame()]})

    assert {:ok, %{records: [%{bins: %{"payload" => "page-1"}}], cursor: %Cursor{} = cursor}} =
             PageRunner.page(tender, query, [])

    Fake.script_stream(
      ctx.fake,
      "A1",
      {:ok, [frame("page-2"), partition_done_frame("page-2"), last_frame()]}
    )

    Fake.script_stream(ctx.fake, "B1", {:ok, [last_frame()]})

    assert {:ok, %{records: [%{bins: %{"payload" => "page-2"}}], cursor: %Cursor{}}} =
             PageRunner.page(tender, query, cursor: cursor)

    Fake.script_stream(ctx.fake, "A1", {:ok, [{:frame, Message.encode_info("status\tok\n")}]})
    Fake.script_stream(ctx.fake, "B1", {:ok, [last_frame()]})

    assert {:error, %Aerospike.Error{code: :parse_error, message: message}} =
             PageRunner.page(tender, query, [])

    assert message =~ "unexpected stream frame type"
  end

  test "page accepts an encoded cursor string", ctx do
    {:ok, tender} = start_tender(ctx)

    cursor =
      %Cursor{
        partitions: [%{id: 10, digest: digest_fixture("cursor"), bval: 12}]
      }
      |> Cursor.encode()

    query =
      Query.new(@namespace, "scan_ops")
      |> Query.where(Filter.range("payload", 0, 9))

    assert {:error, %Aerospike.Error{code: :cluster_not_ready}} =
             PageRunner.page(tender, query, cursor: cursor)
  end

  test "page_node builds scan requests and folds returned records", ctx do
    script_two_node_cluster(ctx.fake)
    {:ok, tender} = start_tender(ctx)
    :ok = Tender.tend_now(tender)

    Fake.script_stream(
      ctx.fake,
      "A1",
      {:ok, [frame("scan-page"), partition_done_frame("scan-page"), last_frame()]}
    )

    assert {:ok, page} = PageRunner.page_node(tender, "A1", Scan.new(@namespace, "scan_ops"), [])
    assert [%{bins: %{"payload" => "scan-page"}}] = page.records
  end

  test "page marks unavailable node partitions and keeps partial results", ctx do
    script_two_node_cluster(ctx.fake)
    {:ok, tender} = start_tender(ctx)
    :ok = Tender.tend_now(tender)

    Fake.script_stream(ctx.fake, "A1", {:error, Aerospike.Error.from_result_code(:pool_timeout)})

    assert {:error, %Aerospike.Error{code: :no_script}} =
             PageRunner.page_node(tender, "A1", Scan.new(@namespace, "scan_ops"), [])
  end

  test "all and all_node follow page cursors until completion", ctx do
    script_two_node_cluster(ctx.fake)
    {:ok, tender} = start_tender(ctx)
    :ok = Tender.tend_now(tender)

    query =
      Query.new(@namespace, "scan_ops")
      |> Query.where(Filter.range("payload", 0, 9))
      |> Query.max_records(1)

    Fake.script_stream(
      ctx.fake,
      "A1",
      {:ok, [frame("page-1"), partition_done_frame("page-1"), last_frame()]}
    )

    Fake.script_stream(
      ctx.fake,
      "A1",
      {:ok, [frame("page-2"), partition_done_frame("page-2"), last_frame()]}
    )

    Fake.script_stream(ctx.fake, "A1", {:ok, [last_frame()]})

    assert {:ok, records} = PageRunner.all_node(tender, "A1", query, [])
    assert Enum.map(records, & &1.bins["payload"]) == ["page-1", "page-2"]

    Fake.script_stream(
      ctx.fake,
      "A1",
      {:ok, [frame("all-1"), partition_done_frame("all-1"), last_frame()]}
    )

    Fake.script_stream(
      ctx.fake,
      "B1",
      {:ok, [last_frame()]}
    )

    Fake.script_stream(
      ctx.fake,
      "A1",
      {:ok, [frame("all-2"), partition_done_frame("all-2"), last_frame()]}
    )

    Fake.script_stream(
      ctx.fake,
      "B1",
      {:ok, [last_frame()]}
    )

    Fake.script_stream(
      ctx.fake,
      "A1",
      {:ok, [last_frame()]}
    )

    Fake.script_stream(
      ctx.fake,
      "B1",
      {:ok, [last_frame()]}
    )

    assert {:ok, all_records} = PageRunner.all(tender, query, [])
    assert Enum.map(all_records, & &1.bins["payload"]) == ["all-1", "all-2"]
  end

  test "page and all validate cursors and max_records before issuing work", ctx do
    script_two_node_cluster(ctx.fake)
    {:ok, tender} = start_tender(ctx)
    :ok = Tender.tend_now(tender)

    query =
      Query.new(@namespace, "scan_ops")
      |> Query.where(Filter.range("payload", 0, 9))
      |> Query.max_records(1)

    assert {:error, %Aerospike.Error{code: :parameter_error, message: message}} =
             PageRunner.page(tender, query, cursor: 123)

    assert message =~ "invalid cursor"

    no_budget_query =
      Query.new(@namespace, "scan_ops")
      |> Query.where(Filter.range("payload", 0, 9))

    assert {:error, %Aerospike.Error{code: :max_records_required}} =
             PageRunner.all(tender, no_budget_query, [])
  end

  test "all and all_node surface recursive page errors from follow-up requests", ctx do
    script_two_node_cluster(ctx.fake)
    {:ok, tender} = start_tender(ctx)
    :ok = Tender.tend_now(tender)

    query =
      Query.new(@namespace, "scan_ops")
      |> Query.where(Filter.range("payload", 0, 9))
      |> Query.max_records(1)

    Fake.script_stream(
      ctx.fake,
      "A1",
      {:ok, [frame("page-1"), partition_done_frame("page-1"), last_frame()]}
    )

    Fake.script_stream(ctx.fake, "A1", {:ok, [{:frame, <<0, 1, 2>>}]})

    assert {:error, %Aerospike.Error{code: :parse_error}} =
             PageRunner.all_node(tender, "A1", query, [])

    Fake.script_stream(
      ctx.fake,
      "A1",
      {:ok, [frame("all-1"), partition_done_frame("all-1"), last_frame()]}
    )

    Fake.script_stream(ctx.fake, "B1", {:ok, [last_frame()]})
    Fake.script_stream(ctx.fake, "A1", {:ok, [{:frame, <<0, 1, 2>>}]})
    Fake.script_stream(ctx.fake, "B1", {:ok, [last_frame()]})

    assert {:error, %Aerospike.Error{code: :parse_error}} = PageRunner.all(tender, query, [])
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
