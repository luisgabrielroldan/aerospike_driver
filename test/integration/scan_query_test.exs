defmodule Aerospike.Integration.ScanQueryTest do
  use ExUnit.Case, async: false

  alias Aerospike.Connection
  alias Aerospike.Cursor
  alias Aerospike.Error
  alias Aerospike.Filter
  alias Aerospike.Page
  alias Aerospike.Query
  alias Aerospike.Record
  alias Aerospike.Scan
  alias Aerospike.Tables
  alias Aerospike.Test.Helpers

  @moduletag :integration

  @namespace "test"
  @set "scan_test_phase8"
  @index_name "idx_age_phase8"

  setup_all do
    host = System.get_env("AEROSPIKE_HOST", "127.0.0.1")
    port = System.get_env("AEROSPIKE_PORT", "3000") |> String.to_integer()
    name = :"scan_query_itest_#{System.unique_integer([:positive])}"

    opts = [
      name: name,
      hosts: ["#{host}:#{port}"],
      pool_size: 2,
      connect_timeout: 5_000,
      recv_timeout: 60_000,
      tend_interval: 60_000,
      defaults: [
        read: [timeout: 5_000],
        write: [timeout: 5_000],
        scan: [timeout: 60_000],
        query: [timeout: 60_000]
      ]
    ]

    {:ok, _sup} = start_supervised({Aerospike, opts})
    await_cluster_ready(name)

    :ok = sindex_create_numeric!(host, port, @namespace, @set, @index_name, "age")

    cities = ["Portland", "Seattle", "Denver"]

    keys =
      for i <- 1..15 do
        key = Aerospike.key(@namespace, @set, "rec_#{i}")

        bins = %{
          "name" => "user_#{i}",
          "age" => 18 + rem(i, 30),
          "city" => Enum.at(cities, rem(i, 3))
        }

        :ok = Aerospike.put!(name, key, bins)
        key
      end

    Process.sleep(100)
    # Secondary index catch-up after writes (SI queries time out if this is too short).
    Process.sleep(900)

    on_exit(fn ->
      Enum.each(keys, &Helpers.cleanup_key(&1, host: host, port: port))
      sindex_delete!(host, port, @namespace, @set, @index_name)
    end)

    {:ok, conn: name, host: host, port: port}
  end

  test "all/2 without max_records returns error", %{conn: conn} do
    scan = Scan.new(@namespace, @set)

    assert {:error, %Error{code: :max_records_required}} = Aerospike.all(conn, scan)
  end

  test "all/2 returns records from scan", %{conn: conn} do
    scan = Scan.new(@namespace, @set) |> Scan.max_records(20)
    assert {:ok, records} = Aerospike.all(conn, scan)
    assert length(records) >= 15
    assert length(records) <= 20
    assert Enum.all?(records, &match?(%Record{}, &1))
  end

  test "all/2 returns all records when max_records exceeds total", %{conn: conn} do
    scan = Scan.new(@namespace, @set) |> Scan.max_records(100)
    assert {:ok, records} = Aerospike.all(conn, scan)
    assert length(records) == 15
    assert Enum.all?(records, &match?(%Record{}, &1))
  end

  test "all/2 respects max_records cap", %{conn: conn} do
    scan = Scan.new(@namespace, @set) |> Scan.max_records(5)
    assert {:ok, records} = Aerospike.all(conn, scan)
    assert length(records) == 5
    assert Enum.all?(records, &match?(%Record{}, &1))
  end

  test "scan with select returns projected bins", %{conn: conn} do
    scan =
      Scan.new(@namespace, @set)
      |> Scan.select(["name"])
      |> Scan.max_records(20)

    assert {:ok, records} = Aerospike.all(conn, scan)
    assert [_ | _] = records

    for r <- records do
      assert Map.has_key?(r.bins, "name")
      refute Map.has_key?(r.bins, "age")
    end
  end

  test "scan namespace-wide", %{conn: conn} do
    scan = Scan.new(@namespace) |> Scan.max_records(5)
    assert {:ok, records} = Aerospike.all(conn, scan)
    assert is_list(records)
    assert length(records) <= 5
  end

  test "count/2 returns record count", %{conn: conn} do
    assert {:ok, n} = Aerospike.count(conn, Scan.new(@namespace, @set))
    assert is_integer(n)
    assert n >= 15
  end

  test "all!/count!/page! return successful values", %{conn: conn} do
    scan = Scan.new(@namespace, @set) |> Scan.max_records(5)

    records = Aerospike.all!(conn, scan)
    assert is_list(records)
    assert length(records) <= 5

    n = Aerospike.count!(conn, Scan.new(@namespace, @set))
    assert is_integer(n)
    assert n >= 15

    page = Aerospike.page!(conn, scan)
    assert %Page{records: page_records, done?: done?} = page
    assert is_list(page_records)
    assert is_boolean(done?)
  end

  test "stream!/2 yields records", %{conn: conn} do
    scan = Scan.new(@namespace, @set) |> Scan.max_records(20)

    records =
      Aerospike.stream!(conn, scan)
      |> Enum.to_list()

    assert match?([_ | _], records)
    assert Enum.all?(records, &match?(%Record{}, &1))
  end

  test "stream!/2 early termination with Enum.take", %{conn: conn} do
    scan = Scan.new(@namespace, @set)

    records =
      Aerospike.stream!(conn, scan)
      |> Enum.take(3)

    assert length(records) == 3
  end

  test "stream!/2 early termination does not crash the caller", %{conn: conn} do
    scan = Scan.new(@namespace, @set)

    records = Aerospike.stream!(conn, scan) |> Enum.take(3)
    assert length(records) == 3

    Process.sleep(50)

    # Caller can still perform CRUD — proves the process wasn't killed by
    # the producer's :shutdown exit signal
    key = Aerospike.key(@namespace, @set, "stream_survival_check")
    :ok = Aerospike.put!(conn, key, %{"alive" => true})
    {:ok, rec} = Aerospike.get(conn, key)
    assert rec.bins["alive"] == true
    Aerospike.delete(conn, key)
  end

  test "stream!/2 sequential early terminations work", %{conn: conn} do
    scan = Scan.new(@namespace, @set)

    for _ <- 1..3 do
      records = Aerospike.stream!(conn, scan) |> Enum.take(2)
      assert length(records) == 2
    end

    Process.sleep(50)

    # Verify caller is healthy after multiple stream halt cycles
    key = Aerospike.key(@namespace, @set, "stream_seq_check")
    :ok = Aerospike.put!(conn, key, %{"ok" => true})
    Aerospike.delete(conn, key)
  end

  test "stream!/2 early termination in spawned process exits normally", %{conn: conn} do
    scan = Scan.new(@namespace, @set)
    test_pid = self()

    pid =
      spawn(fn ->
        records = Aerospike.stream!(conn, scan) |> Enum.take(3)
        send(test_pid, {:result, records})
      end)

    ref = Process.monitor(pid)

    assert_receive {:result, records}, 10_000
    assert length(records) == 3

    assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 5_000
  end

  test "stream!/2 no EXIT messages leak into caller mailbox", %{conn: conn} do
    scan = Scan.new(@namespace, @set)
    _records = Aerospike.stream!(conn, scan) |> Enum.take(3)

    Process.sleep(100)

    receive do
      {:EXIT, _pid, _reason} -> flunk("leaked EXIT message in caller mailbox")
    after
      0 -> :ok
    end
  end

  test "stream!/2 full consumption followed by more operations", %{conn: conn} do
    scan = Scan.new(@namespace, @set) |> Scan.max_records(5)

    records = Aerospike.stream!(conn, scan) |> Enum.to_list()
    assert length(records) <= 5

    # After full consumption, the producer exited naturally. Verify caller is fine.
    key = Aerospike.key(@namespace, @set, "stream_full_check")
    :ok = Aerospike.put!(conn, key, %{"done" => true})
    {:ok, rec} = Aerospike.get(conn, key)
    assert rec.bins["done"] == true
    Aerospike.delete(conn, key)
  end

  test "page/3 returns page with cursor", %{conn: conn} do
    scan = Scan.new(@namespace, @set) |> Scan.max_records(5)
    assert {:ok, page} = Aerospike.page(conn, scan)
    assert %Page{records: recs, cursor: _cursor, done?: done?} = page
    assert is_list(recs)
    assert length(recs) <= 5
    assert is_boolean(done?)
  end

  test "page/3 pagination continues with cursor", %{conn: conn} do
    scan = Scan.new(@namespace, @set) |> Scan.max_records(5)
    assert {:ok, page1} = Aerospike.page(conn, scan)
    assert is_list(page1.records)

    if page1.done? do
      assert page1.cursor == nil
    else
      assert %Cursor{} = page1.cursor
      assert {:ok, page2} = Aerospike.page(conn, scan, cursor: page1.cursor)
      assert is_list(page2.records)
    end
  end

  test "cursor encode/decode roundtrip works", %{conn: conn} do
    scan = Scan.new(@namespace, @set) |> Scan.max_records(5)
    assert {:ok, page1} = Aerospike.page(conn, scan)

    unless page1.done? do
      encoded = Cursor.encode(page1.cursor)
      assert {:ok, decoded} = Cursor.decode(encoded)

      assert {:ok, page2} = Aerospike.page(conn, scan, cursor: decoded)
      assert is_list(page2.records)
    end
  end

  test "query with secondary index", %{conn: conn} do
    base = Filter.range("age", 20, 25)
    flt = %{base | index_name: @index_name}

    query =
      Query.new(@namespace, @set)
      |> Query.where(flt)
      |> Query.max_records(50)

    assert {:ok, records} = Aerospike.all(conn, query)

    for r <- records do
      age = r.bins["age"]
      assert is_integer(age)
      assert age >= 20 and age <= 25
    end
  end

  defp await_cluster_ready(name, timeout \\ 5_000) do
    deadline = System.monotonic_time(:millisecond) + timeout
    await_cluster_ready_loop(name, deadline)
  end

  defp await_cluster_ready_loop(name, deadline) do
    cond do
      cluster_ready?(name) ->
        :ok

      System.monotonic_time(:millisecond) > deadline ->
        flunk("cluster not ready")

      true ->
        Process.sleep(50)
        await_cluster_ready_loop(name, deadline)
    end
  end

  defp cluster_ready?(name) do
    match?([{_, true}], :ets.lookup(Tables.meta(name), Tables.ready_key()))
  end

  defp sindex_create_numeric!(host, port, ns, set, index_name, bin) do
    cmd =
      "sindex-create:namespace=#{ns};set=#{set};indexname=#{index_name};bin=#{bin};type=numeric;indextype=default"

    {:ok, conn} = Connection.connect(host: host, port: port)
    {:ok, conn} = Connection.login(conn)

    try do
      {:ok, _conn, map} = Connection.request_info(conn, [cmd])
      assert_sindex_command_ok!(map, cmd)
    after
      Connection.close(conn)
    end
  end

  defp sindex_delete!(host, port, ns, set, index_name) do
    cmd = "sindex-delete:namespace=#{ns};set=#{set};indexname=#{index_name}"

    case Connection.connect(host: host, port: port) do
      {:ok, conn} ->
        {:ok, conn} = Connection.login(conn)

        try do
          _ = Connection.request_info(conn, [cmd])
        after
          Connection.close(conn)
        end

      _ ->
        :ok
    end
  end

  defp assert_sindex_command_ok!(map, cmd) when is_map(map) do
    val = sindex_response_value(map, cmd)

    if sindex_create_success?(val) do
      :ok
    else
      flunk("sindex command failed (#{cmd}): #{inspect(val)}")
    end
  end

  defp sindex_response_value(map, cmd) do
    case Map.get(map, cmd) do
      nil when map_size(map) == 1 ->
        [{_k, v}] = Map.to_list(map)
        v

      v ->
        v
    end
  end

  defp sindex_create_success?(v) when is_binary(v) do
    t = String.downcase(v)
    t == "ok" or String.contains?(t, "already")
  end

  defp sindex_create_success?(_), do: false
end
