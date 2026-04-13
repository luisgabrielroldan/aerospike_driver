defmodule Aerospike.Integration.AdminTest do
  use ExUnit.Case, async: false

  alias Aerospike.Tables

  @moduletag :integration

  setup do
    host = System.get_env("AEROSPIKE_HOST", "127.0.0.1")
    port = System.get_env("AEROSPIKE_PORT", "3000") |> String.to_integer()
    name = :"admin_itest_#{System.unique_integer([:positive])}"

    {:ok, _sup} =
      start_supervised(
        {Aerospike,
         name: name,
         hosts: ["#{host}:#{port}"],
         pool_size: 2,
         connect_timeout: 5_000,
         tend_interval: 60_000}
      )

    await_cluster_ready(name)
    {:ok, conn: name}
  end

  describe "info/2" do
    test "returns non-empty response for namespaces command", %{conn: conn} do
      assert {:ok, response} = Aerospike.info(conn, "namespaces")
      assert is_binary(response)
      assert String.length(response) > 0
    end

    test "returns response for build command", %{conn: conn} do
      assert {:ok, response} = Aerospike.info(conn, "build")
      assert is_binary(response)
    end

    test "accepts pool_checkout_timeout opt", %{conn: conn} do
      assert {:ok, _} = Aerospike.info(conn, "namespaces", pool_checkout_timeout: 5_000)
    end
  end

  describe "info_node/3" do
    test "returns response when targeting a specific node by name", %{conn: conn} do
      assert {:ok, names} = Aerospike.node_names(conn)
      assert [node_name | _] = names

      assert {:ok, response} = Aerospike.info_node(conn, node_name, "build")
      assert is_binary(response)
    end

    test "returns error for unknown node name", %{conn: conn} do
      assert {:error, %Aerospike.Error{}} = Aerospike.info_node(conn, "no-such-node", "build")
    end
  end

  describe "nodes/1" do
    test "returns at least one node with name, host, and port", %{conn: conn} do
      assert {:ok, nodes} = Aerospike.nodes(conn)
      assert nodes != []
      assert [%{name: name, host: host, port: port} | _] = nodes
      assert is_binary(name)
      assert is_binary(host)
      assert is_integer(port) and port > 0
    end
  end

  describe "node_names/1" do
    test "returns at least one node name string", %{conn: conn} do
      assert {:ok, names} = Aerospike.node_names(conn)
      assert names != []
      assert Enum.all?(names, &is_binary/1)
    end

    test "node_names and nodes return consistent sets", %{conn: conn} do
      assert {:ok, nodes} = Aerospike.nodes(conn)
      assert {:ok, names} = Aerospike.node_names(conn)
      node_names_from_nodes = nodes |> Enum.map(& &1.name) |> Enum.sort()
      assert Enum.sort(names) == node_names_from_nodes
    end
  end

  describe "truncate/2" do
    test "returns :ok for a valid namespace", %{conn: conn} do
      assert :ok = Aerospike.truncate(conn, "test")
    end
  end

  describe "truncate/3 (set)" do
    test "truncates only records in the specified set", %{conn: conn} do
      set = "admin_trunc_set_#{System.unique_integer([:positive])}"
      other_set = "admin_trunc_other_#{System.unique_integer([:positive])}"

      trunc_key = Aerospike.key("test", set, "trunc")
      keep_key = Aerospike.key("test", other_set, "keep")

      assert :ok = Aerospike.put(conn, trunc_key, %{"v" => 1})
      assert :ok = Aerospike.put(conn, keep_key, %{"v" => 2})
      assert {:ok, _} = Aerospike.get(conn, trunc_key)
      assert {:ok, _} = Aerospike.get(conn, keep_key)

      Process.sleep(5)

      assert :ok = Aerospike.truncate(conn, "test", set)

      assert_eventually(
        fn ->
          trunc_state = Aerospike.get(conn, trunc_key)
          keep_state = Aerospike.get(conn, keep_key)

          condition? =
            match?({:error, %Aerospike.Error{code: :key_not_found}}, trunc_state) and
              match?({:ok, _}, keep_state)

          {condition?,
           "truncate key still present or keep key missing: trunc=#{inspect(trunc_state)} keep=#{inspect(keep_state)}"}
        end,
        timeout: truncate_wait_timeout_ms(),
        interval: 200
      )
    end
  end

  defp assert_eventually(fun, opts) when is_function(fun, 0) and is_list(opts) do
    timeout = Keyword.fetch!(opts, :timeout)
    interval = Keyword.get(opts, :interval, 200)
    deadline = System.monotonic_time(:millisecond) + timeout
    assert_eventually_loop(fun, deadline, interval, nil)
  end

  defp assert_eventually_loop(fun, deadline, interval, last_message) do
    case fun.() do
      true ->
        :ok

      {true, _message} ->
        :ok

      false ->
        await_or_flunk(fun, deadline, interval, last_message || "condition returned false")

      {false, message} when is_binary(message) ->
        await_or_flunk(fun, deadline, interval, message)
    end
  end

  defp await_or_flunk(fun, deadline, interval, message) do
    if System.monotonic_time(:millisecond) > deadline do
      flunk("condition did not become true within timeout: #{message}")
    else
      Process.sleep(interval)
      assert_eventually_loop(fun, deadline, interval, message)
    end
  end

  defp truncate_wait_timeout_ms do
    if System.get_env("CI") == "true", do: 30_000, else: 10_000
  end

  defp await_cluster_ready(name, timeout \\ 5_000) do
    deadline = System.monotonic_time(:millisecond) + timeout
    await_cluster_ready_loop(name, deadline)
  end

  defp await_cluster_ready_loop(name, deadline) do
    cond do
      match?([{_, true}], :ets.lookup(Tables.meta(name), Tables.ready_key())) ->
        :ok

      System.monotonic_time(:millisecond) > deadline ->
        flunk("cluster not ready within timeout")

      true ->
        Process.sleep(50)
        await_cluster_ready_loop(name, deadline)
    end
  end
end
