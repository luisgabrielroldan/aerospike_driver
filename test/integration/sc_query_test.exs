defmodule Aerospike.Integration.SCQueryTest do
  @moduledoc """
  Integration tests for queries with secondary-index filters on Strong Consistency
  (SC) namespaces. Reproduces failures described in the SC-mode query bug.

  Requires Aerospike Enterprise Edition with strong-consistency enabled.
  Start with:

      docker compose --profile enterprise up -d

  Run with:

      mix test --include integration --include enterprise
  """
  use ExUnit.Case, async: true

  alias Aerospike.Connection
  alias Aerospike.Exp
  alias Aerospike.Filter
  alias Aerospike.Query
  alias Aerospike.Record
  alias Aerospike.Scan
  alias Aerospike.Test.Helpers

  @moduletag :enterprise

  @namespace "test"
  @set "sc_query_bug"
  @index_name "idx_age_sc_query"

  setup_all do
    host = System.get_env("AEROSPIKE_EE_HOST", "127.0.0.1")
    port = System.get_env("AEROSPIKE_EE_PORT", "3100") |> String.to_integer()

    unless ee_running?(host, port) do
      flunk("""
      Enterprise server not running on #{host}:#{port}. Start with:

          docker compose --profile enterprise up -d
      """)
    end

    name = :"sc_query_itest_#{System.unique_integer([:positive])}"

    opts = [
      name: name,
      hosts: ["#{host}:#{port}"],
      pool_size: 2,
      connect_timeout: 5_000,
      recv_timeout: 10_000,
      tend_interval: 60_000,
      defaults: [
        read: [timeout: 5_000],
        write: [timeout: 5_000],
        scan: [timeout: 30_000],
        query: [timeout: 30_000]
      ]
    ]

    {:ok, _sup} = start_supervised({Aerospike, opts})
    Helpers.await_cluster_ready(name)

    :ok = sindex_create_numeric!(host, port, @namespace, @set, @index_name, "age")

    depts = ["engineering", "sales", "marketing"]

    keys =
      for i <- 1..20 do
        key = Aerospike.key(@namespace, @set, "sc_rec_#{i}")

        bins = %{
          "name" => "user_#{i}",
          "age" => 20 + rem(i, 10),
          "dept" => Enum.at(depts, rem(i, 3))
        }

        :ok = Aerospike.put!(name, key, bins)
        key
      end

    Process.sleep(1_500)

    on_exit(fn ->
      Enum.each(keys, &Helpers.cleanup_key(&1, host: host, port: port))
      sindex_delete!(host, port, @namespace, @set, @index_name)
    end)

    {:ok, conn: name, host: host, port: port}
  end

  # ---------------------------------------------------------------------------
  # Baseline: scans work on SC (sanity check)
  # ---------------------------------------------------------------------------

  test "scan all/2 works on SC namespace", %{conn: conn} do
    scan = Scan.new(@namespace, @set) |> Scan.max_records(50)
    assert {:ok, records} = Aerospike.all(conn, scan)
    assert length(records) >= 20
    assert Enum.all?(records, &match?(%Record{}, &1))
  end

  test "scan stream!/2 works on SC namespace", %{conn: conn} do
    scan = Scan.new(@namespace, @set) |> Scan.max_records(50)
    records = Aerospike.stream!(conn, scan) |> Enum.to_list()
    assert length(records) >= 20
    assert Enum.all?(records, &match?(%Record{}, &1))
  end

  # ---------------------------------------------------------------------------
  # Working case: query with SI filter (no expression) via all/3
  # ---------------------------------------------------------------------------

  test "query all/3 with SI filter (no expression) works on SC", %{conn: conn} do
    flt = %{Filter.range("age", 20, 25) | index_name: @index_name}

    query =
      Query.new(@namespace, @set)
      |> Query.where(flt)
      |> Query.max_records(50)

    assert {:ok, records} = Aerospike.all(conn, query)
    assert [_ | _] = records

    for r <- records do
      age = r.bins["age"]
      assert age >= 20 and age <= 25
    end
  end

  # ---------------------------------------------------------------------------
  # Bug Path A: query all/3 with SI filter + expression filter fails on SC
  # Previously returned {:error, :parse_error, "expected LAST frame"} because
  # the terminal frame's non-zero RC was not recognized as a stream terminator.
  # ---------------------------------------------------------------------------

  test "query all/3 with SI filter + expression filter on SC", %{conn: conn} do
    flt = %{Filter.range("age", 20, 29) | index_name: @index_name}

    query =
      Query.new(@namespace, @set)
      |> Query.where(flt)
      |> Query.filter(Exp.eq(Exp.str_bin("dept"), Exp.val("engineering")))
      |> Query.max_records(50)

    assert {:ok, records} = Aerospike.all(conn, query)
    assert is_list(records)

    for r <- records do
      age = r.bins["age"]
      assert age >= 20 and age <= 29
      assert r.bins["dept"] == "engineering"
    end
  end

  # ---------------------------------------------------------------------------
  # Bug Path B: query stream!/2 with SI filter fails on SC
  # Previously timed out because recv_frame loop didn't detect stream termination.
  # ---------------------------------------------------------------------------

  test "query stream!/2 with SI filter (no expression) on SC", %{conn: conn} do
    flt = %{Filter.range("age", 20, 25) | index_name: @index_name}

    query =
      Query.new(@namespace, @set)
      |> Query.where(flt)
      |> Query.max_records(50)

    records = Aerospike.stream!(conn, query) |> Enum.to_list()
    assert [_ | _] = records

    for r <- records do
      age = r.bins["age"]
      assert age >= 20 and age <= 25
    end
  end

  test "query stream!/2 with SI filter + expression filter on SC", %{conn: conn} do
    flt = %{Filter.range("age", 20, 29) | index_name: @index_name}

    query =
      Query.new(@namespace, @set)
      |> Query.where(flt)
      |> Query.filter(Exp.eq(Exp.str_bin("dept"), Exp.val("engineering")))
      |> Query.max_records(50)

    records = Aerospike.stream!(conn, query) |> Enum.to_list()
    assert is_list(records)

    for r <- records do
      age = r.bins["age"]
      assert age >= 20 and age <= 29
      assert r.bins["dept"] == "engineering"
    end
  end

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp ee_running?(host, port) do
    case Connection.connect(host: host, port: port, timeout: 2_000) do
      {:ok, conn} ->
        Connection.close(conn)
        true

      {:error, _} ->
        false
    end
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
