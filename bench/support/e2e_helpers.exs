defmodule Aerospike.Bench.Support.E2EHelpers do
  @moduledoc false

  alias Aerospike.Key
  alias Aerospike.Tender

  def verify_server_available!(host, port) do
    case :gen_tcp.connect(String.to_charlist(host), port, [:binary, active: false], 2_000) do
      {:ok, socket} ->
        :gen_tcp.close(socket)
        :ok

      {:error, reason} ->
        raise """
        Aerospike server unavailable at #{host}:#{port}: #{inspect(reason)}.
        Start it first (for example, `docker compose up -d`) and retry the benchmark.
        """
    end
  end

  def start_client!(conn_name, host, port, opts \\ []) do
    stop_client(conn_name)

    start_opts =
      [
        name: conn_name,
        transport: Aerospike.Transport.Tcp,
        hosts: ["#{host}:#{port}"],
        namespaces: [Keyword.get(opts, :namespace, "test")],
        tend_trigger: :manual,
        pool_size: Keyword.get(opts, :pool_size, 16)
      ]

    case Aerospike.start_link(start_opts) do
      {:ok, _pid} ->
        :ok = Tender.tend_now(conn_name)
        :ok

      {:error, reason} ->
        raise "failed to start benchmark client #{inspect(conn_name)}: #{inspect(reason)}"
    end
  end

  def with_started_client(conn_name, host, port, namespace, fun, opts \\ [])
      when is_binary(namespace) and is_function(fun, 0) do
    verify_server_available!(host, port)
    start_client!(conn_name, host, port, Keyword.put(opts, :namespace, namespace))

    try do
      true = Tender.ready?(conn_name)
      fun.()
    after
      stop_client(conn_name)
    end
  end

  def stop_client(conn_name) do
    Supervisor.stop(Aerospike.Supervisor.sup_name(conn_name))
  rescue
    _ -> :ok
  catch
    :exit, _ -> :ok
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
      Key.new(namespace, set, "#{prefix}:#{index}")
    end)
  end

  def bootstrap_keys!(conn_name, keys, payload) when is_list(keys) and is_map(payload) do
    Enum.each(keys, fn key ->
      {:ok, _metadata} = Aerospike.put(conn_name, key, payload)
    end)
  end

  def bootstrap_records!(conn_name, entries) when is_list(entries) do
    Enum.each(entries, fn {key, bins} ->
      {:ok, _metadata} = Aerospike.put(conn_name, key, bins)
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
end
