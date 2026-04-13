defmodule Aerospike.Integration.CircuitBreakerTest do
  use ExUnit.Case, async: false

  alias Aerospike.CircuitBreaker
  alias Aerospike.Error
  alias Aerospike.Protocol.AsmMsg
  alias Aerospike.Protocol.Response
  alias Aerospike.Router
  alias Aerospike.Tables
  alias Aerospike.Test.Helpers

  @moduletag :integration

  setup do
    host = System.get_env("AEROSPIKE_HOST", "127.0.0.1")
    port = System.get_env("AEROSPIKE_PORT", "3000") |> String.to_integer()
    %{host: host, port: port}
  end

  test "allow_request?/2 rejects at threshold for active cluster node", %{host: host, port: port} do
    name = start_cluster(host, port, max_error_rate: 5, error_rate_window: 5)
    node_name = first_node_name(name)
    :ets.insert(Tables.breaker(name), {node_name, 5})

    assert {:error, %Error{code: :max_error_rate, node: ^node_name}} =
             CircuitBreaker.allow_request?(name, node_name)
  end

  test "router run rejects before checkout when breaker is open", %{host: host, port: port} do
    name = start_cluster(host, port, max_error_rate: 5, error_rate_window: 5)
    key = Helpers.unique_key("test", "circuit_breaker_router_reject")
    partition_id = Aerospike.Key.partition_id(key)

    {:ok, _pool_pid, node_name} =
      Router.resolve_pool_for_partition(name, key.namespace, partition_id, 0)

    :ets.insert(Tables.breaker(name), {node_name, 5})

    wire_put = Helpers.put_wire(key, %{"x" => 1})

    assert {:error, %Error{code: :max_error_rate, node: ^node_name}} =
             Router.run(name, key, wire_put, pool_checkout_timeout: 5_000)
  end

  test "counter resets on tend window boundary", %{host: host, port: port} do
    name =
      start_cluster(host, port, max_error_rate: 5, error_rate_window: 1, tend_interval: 100)

    node_name = first_node_name(name)
    :ets.insert(Tables.breaker(name), {node_name, 3})

    poll_until(2_000, "breaker counter did not reset", fn ->
      :ets.lookup(Tables.breaker(name), node_name) == []
    end)
  end

  test "max_error_rate 0 keeps requests allowed", %{host: host, port: port} do
    name = start_cluster(host, port, max_error_rate: 0, error_rate_window: 1)
    node_name = first_node_name(name)
    :ets.insert(Tables.breaker(name), {node_name, 999})

    assert :ok = CircuitBreaker.allow_request?(name, node_name)

    key = Helpers.unique_key("test", "circuit_breaker_itest")
    on_exit(fn -> Helpers.cleanup_key(key, host: host, port: port) end)

    wire_put = Helpers.put_wire(key, %{"x" => 1})
    assert {:ok, body, _node} = Router.run(name, key, wire_put, pool_checkout_timeout: 5_000)
    assert {:ok, msg} = AsmMsg.decode(body)
    assert :ok = Response.parse_write_response(msg)
  end

  defp start_cluster(host, port, opts) do
    name = :"circuit_breaker_itest_#{System.unique_integer([:positive])}"

    base_opts = [
      name: name,
      hosts: ["#{host}:#{port}"],
      pool_size: 2,
      connect_timeout: 5_000,
      tend_interval: 60_000
    ]

    {:ok, _sup} = start_supervised({Aerospike.Supervisor, Keyword.merge(base_opts, opts)})
    Helpers.await_cluster_ready(name)
    name
  end

  defp first_node_name(name) do
    [{node_name, _row} | _] = :ets.tab2list(Tables.nodes(name))
    node_name
  end

  defp poll_until(timeout, message, check_fn) do
    deadline = System.monotonic_time(:millisecond) + timeout
    poll_loop(deadline, timeout, message, check_fn)
  end

  defp poll_loop(deadline, timeout, message, check_fn) do
    cond do
      check_fn.() ->
        :ok

      System.monotonic_time(:millisecond) > deadline ->
        flunk("#{message} within #{timeout}ms")

      true ->
        Process.sleep(50)
        poll_loop(deadline, timeout, message, check_fn)
    end
  end
end
