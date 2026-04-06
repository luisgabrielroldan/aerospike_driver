defmodule Aerospike.Integration.ExpressionsTest do
  use ExUnit.Case, async: false

  alias Aerospike.Error
  alias Aerospike.Exp
  alias Aerospike.Op
  alias Aerospike.Scan
  alias Aerospike.Tables
  alias Aerospike.Test.Helpers

  @moduletag :integration

  @namespace "test"
  @set "expr_test_phase9"

  setup do
    host = System.get_env("AEROSPIKE_HOST", "127.0.0.1")
    port = System.get_env("AEROSPIKE_PORT", "3000") |> String.to_integer()
    name = :"expr_itest_#{System.unique_integer([:positive])}"

    opts = [
      name: name,
      hosts: ["#{host}:#{port}"],
      pool_size: 2,
      connect_timeout: 5_000,
      tend_interval: 60_000,
      defaults: [
        read: [timeout: 5_000],
        write: [timeout: 5_000],
        operate: [timeout: 5_000],
        scan: [timeout: 30_000]
      ]
    ]

    {:ok, _sup} = start_supervised({Aerospike, opts})
    await_cluster_ready(name)

    {:ok, conn: name, host: host, port: port}
  end

  # ---------------------------------------------------------------------------
  # Test 1: get with a matching filter returns the record
  # ---------------------------------------------------------------------------

  test "get with matching filter expression returns record", %{conn: conn, host: host, port: port} do
    key = Aerospike.key(@namespace, @set, "expr_get_match_#{System.unique_integer([:positive])}")
    on_exit(fn -> Helpers.cleanup_key(key, host: host, port: port) end)

    :ok = Aerospike.put!(conn, key, %{"age" => 30, "status" => "active"})

    filter = Exp.gt(Exp.int_bin("age"), Exp.int(20))
    assert {:ok, record} = Aerospike.get(conn, key, filter: filter)
    assert record.bins["age"] == 30
    assert record.bins["status"] == "active"
  end

  # ---------------------------------------------------------------------------
  # Test 1 (cont.): get with a non-matching filter returns filtered_out
  # ---------------------------------------------------------------------------

  test "get with non-matching filter expression returns filtered_out", %{
    conn: conn,
    host: host,
    port: port
  } do
    key =
      Aerospike.key(@namespace, @set, "expr_get_nomatch_#{System.unique_integer([:positive])}")

    on_exit(fn -> Helpers.cleanup_key(key, host: host, port: port) end)

    :ok = Aerospike.put!(conn, key, %{"age" => 15})

    filter = Exp.gt(Exp.int_bin("age"), Exp.int(20))
    assert {:error, %Error{code: :filtered_out}} = Aerospike.get(conn, key, filter: filter)
  end

  # ---------------------------------------------------------------------------
  # Test 2: scan with filter expression returns only matching records
  # ---------------------------------------------------------------------------

  test "scan with filter expression returns only matching records", %{
    conn: conn,
    host: host,
    port: port
  } do
    suffix = System.unique_integer([:positive])

    keys =
      for i <- 1..6 do
        key = Aerospike.key(@namespace, @set, "expr_scan_#{suffix}_#{i}")
        :ok = Aerospike.put!(conn, key, %{"score" => i * 10})
        key
      end

    on_exit(fn -> Enum.each(keys, &Helpers.cleanup_key(&1, host: host, port: port)) end)

    # Only records with score > 30 should be returned (i = 4, 5, 6 → scores 40, 50, 60)
    filter = Exp.gt(Exp.int_bin("score"), Exp.int(30))

    scan =
      Scan.new(@namespace, @set)
      |> Scan.filter(filter)
      |> Scan.max_records(20)

    assert {:ok, records} = Aerospike.all(conn, scan)

    # Filter by the keys we just wrote to avoid picking up stale data from other tests
    our_digests = MapSet.new(keys, & &1.digest)
    our_records = Enum.filter(records, &MapSet.member?(our_digests, &1.key.digest))

    assert length(our_records) == 3

    for r <- our_records do
      assert r.bins["score"] > 30
    end
  end

  # ---------------------------------------------------------------------------
  # Test 3: Op.Exp.read returns server-computed value in response bins
  # ---------------------------------------------------------------------------

  test "Op.Exp.read returns server-side expression result in bins", %{
    conn: conn,
    host: host,
    port: port
  } do
    key = Aerospike.key(@namespace, @set, "expr_read_#{System.unique_integer([:positive])}")
    on_exit(fn -> Helpers.cleanup_key(key, host: host, port: port) end)

    :ok = Aerospike.put!(conn, key, %{"count" => 7})

    # Read the "count" bin server-side via an expression and surface it as "result"
    assert {:ok, record} =
             Aerospike.operate(conn, key, [
               Op.Exp.read("result", Exp.int_bin("count"))
             ])

    assert record.bins["result"] == 7
  end

  # ---------------------------------------------------------------------------
  # Test 4: Op.Exp.write stores a server-computed value; get confirms it
  # ---------------------------------------------------------------------------

  test "Op.Exp.write stores server-side expression result in a bin", %{
    conn: conn,
    host: host,
    port: port
  } do
    key = Aerospike.key(@namespace, @set, "expr_write_#{System.unique_integer([:positive])}")
    on_exit(fn -> Helpers.cleanup_key(key, host: host, port: port) end)

    :ok = Aerospike.put!(conn, key, %{"seed" => 0})

    # Write a literal constant (99) to "computed" using an expression write op
    assert {:ok, _} =
             Aerospike.operate(conn, key, [
               Op.Exp.write("computed", Exp.int(99))
             ])

    assert {:ok, record} = Aerospike.get(conn, key)
    assert record.bins["computed"] == 99
  end

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp await_cluster_ready(name, timeout \\ 5_000) do
    deadline = System.monotonic_time(:millisecond) + timeout
    await_cluster_ready_loop(name, deadline)
  end

  defp await_cluster_ready_loop(name, deadline) do
    cond do
      cluster_ready?(name) ->
        :ok

      System.monotonic_time(:millisecond) > deadline ->
        flunk("cluster not ready within timeout")

      true ->
        Process.sleep(50)
        await_cluster_ready_loop(name, deadline)
    end
  end

  defp cluster_ready?(name) do
    match?([{_, true}], :ets.lookup(Tables.meta(name), Tables.ready_key()))
  end
end
