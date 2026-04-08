defmodule Aerospike.Bench.Support.E2EHelpers do
  @moduledoc false

  alias Aerospike.Connection
  alias Aerospike.Tables

  def verify_server_available!(host, port) do
    case Connection.connect(host: host, port: port, timeout: 2_000) do
      {:ok, conn} ->
        case Connection.login(conn) do
          {:ok, logged} ->
            Connection.close(logged)
            :ok

          {:error, reason} ->
            Connection.close(conn)

            raise """
            Aerospike server is reachable at #{host}:#{port}, but login failed: #{inspect(reason)}.
            Ensure a local Aerospike server is running and accepts client authentication.
            """
        end

      {:error, reason} ->
        raise """
        Aerospike server unavailable at #{host}:#{port}: #{inspect(reason)}.
        Start it first (for example, `docker compose up -d`) and retry the benchmark.
        """
    end
  end

  def start_client!(conn_name, host, port, pool_size \\ 16) do
    stop_client(conn_name)

    case Aerospike.start_link(name: conn_name, hosts: ["#{host}:#{port}"], pool_size: pool_size) do
      {:ok, _pid} ->
        :ok

      {:error, reason} ->
        raise "failed to start benchmark client #{inspect(conn_name)}: #{inspect(reason)}"
    end
  end

  def with_started_client(conn_name, host, port, timeout_ms, fun)
      when is_function(fun, 0) and is_integer(timeout_ms) and timeout_ms > 0 do
    verify_server_available!(host, port)
    start_client!(conn_name, host, port)
    await_cluster_ready!(conn_name, timeout_ms)

    try do
      fun.()
    after
      stop_client(conn_name)
    end
  end

  def stop_client(conn_name) do
    Aerospike.close(conn_name)
  rescue
    _ -> :ok
  catch
    :exit, _ -> :ok
  end

  def await_cluster_ready!(conn_name, timeout_ms)
      when is_integer(timeout_ms) and timeout_ms > 0 do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    wait_for_cluster_ready(conn_name, deadline, timeout_ms)
  end

  def payload(payload_profiles, profile_name)
      when is_map(payload_profiles) and is_atom(profile_name) do
    profile = Map.fetch!(payload_profiles, profile_name)
    bin_name = Map.fetch!(profile, :bin)
    size_bytes = Map.fetch!(profile, :bytes)

    %{bin_name => String.duplicate("x", size_bytes)}
  end

  def key_ring(namespace, set, prefix, count)
      when is_binary(namespace) and is_binary(set) and is_binary(prefix) and is_integer(count) and
             count > 0 do
    Enum.map(1..count, fn index ->
      Aerospike.key(namespace, set, "#{prefix}:#{index}")
    end)
  end

  def ring_keys({items, size}) when is_tuple(items) and is_integer(size) and size > 0 do
    Tuple.to_list(items)
  end

  def bootstrap_keys!(conn_name, keys, payload) when is_list(keys) and is_map(payload) do
    Enum.each(keys, fn key ->
      :ok = Aerospike.put(conn_name, key, payload)
    end)
  end

  def teardown_keys(conn_name, keys) when is_list(keys) do
    Enum.each(keys, fn key ->
      case Aerospike.delete(conn_name, key) do
        {:ok, _deleted?} -> :ok
        {:error, _reason} -> :ok
      end
    end)
  end

  defp wait_for_cluster_ready(conn_name, deadline, timeout_ms) do
    case cluster_ready?(conn_name) do
      true ->
        :ok

      false ->
        wait_or_raise(conn_name, deadline, timeout_ms)
    end
  end

  defp wait_or_raise(conn_name, deadline, timeout_ms) do
    case System.monotonic_time(:millisecond) > deadline do
      true ->
        raise "cluster did not become ready within #{timeout_ms}ms"

      false ->
        Process.sleep(50)
        wait_for_cluster_ready(conn_name, deadline, timeout_ms)
    end
  end

  defp cluster_ready?(conn_name) do
    case :ets.whereis(Tables.meta(conn_name)) do
      :undefined ->
        false

      _ ->
        match?([{_, true}], :ets.lookup(Tables.meta(conn_name), Tables.ready_key()))
    end
  end
end
