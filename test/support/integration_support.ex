defmodule Aerospike.Test.IntegrationSupport do
  @moduledoc false

  alias Aerospike.Key
  alias Aerospike.Transport.Tcp

  @default_probe_interval_ms 100

  def unique_name(prefix) when is_binary(prefix) do
    suffix =
      :crypto.strong_rand_bytes(4)
      |> Base.encode16(case: :lower)

    "#{prefix}_#{System.system_time(:microsecond)}_#{suffix}"
  end

  def unique_atom(prefix) when is_binary(prefix) do
    prefix
    |> unique_name()
    |> String.to_atom()
  end

  def unique_key(namespace, set, prefix)
      when is_binary(namespace) and is_binary(set) and is_binary(prefix) do
    Key.new(namespace, set, unique_name(prefix))
  end

  def stop_supervisor_quietly(sup) do
    try do
      Supervisor.stop(sup)
    catch
      :exit, _ -> :ok
    end
  end

  def probe_aerospike!(host, port, message_suffix)
      when is_binary(host) and is_integer(port) and is_binary(message_suffix) do
    case :gen_tcp.connect(to_charlist(host), port, [:binary, active: false], 1_000) do
      {:ok, sock} ->
        :gen_tcp.close(sock)
        :ok

      {:error, reason} ->
        raise "Aerospike not reachable at #{host}:#{port} (#{inspect(reason)}). #{message_suffix}"
    end
  end

  def probe_aerospike!(host, port) when is_binary(host) and is_integer(port) do
    probe_aerospike!(host, port, "Run `docker compose up -d` first.")
  end

  def probe_aerospike!(seeds, message_suffix)
      when is_list(seeds) and is_binary(message_suffix) do
    Enum.each(seeds, fn {host, port} -> probe_aerospike!(host, port, message_suffix) end)
  end

  def wait_for_seed_ready!(
        host,
        port,
        namespace,
        timeout_ms,
        interval_ms \\ @default_probe_interval_ms
      )
      when is_binary(host) and is_integer(port) and is_binary(namespace) and
             is_integer(timeout_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms

    wait_until!(
      deadline,
      fn -> seed_client_ready?({host, port}, namespace) end,
      "expected #{host}:#{port} to serve partition-generation and replicas for namespace #{namespace}",
      interval_ms
    )
  end

  def wait_for_cluster_ready!(
        seeds,
        namespace,
        timeout_ms,
        opts \\ []
      )
      when is_list(seeds) and is_binary(namespace) and is_integer(timeout_ms) and is_list(opts) do
    expected_size = Keyword.get(opts, :expected_size, length(seeds))
    stable_container = Keyword.get(opts, :stable_container, "aerospike1")
    ignore_migrations = Keyword.get(opts, :ignore_migrations, false)
    interval_ms = Keyword.get(opts, :interval_ms, @default_probe_interval_ms)
    deadline = System.monotonic_time(:millisecond) + timeout_ms

    wait_until!(
      deadline,
      fn ->
        Enum.all?(seeds, &seed_client_ready?(&1, namespace)) and
          cluster_stable?(expected_size, stable_container, ignore_migrations: ignore_migrations)
      end,
      "expected the local Aerospike cluster to serve all seeds and report cluster-stable:size=#{expected_size}",
      interval_ms
    )
  end

  def wait_for_tender_ready!(cluster, timeout_ms, interval_ms \\ @default_probe_interval_ms)
      when is_atom(cluster) and is_integer(timeout_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_wait_for_tender_ready(cluster, deadline, interval_ms)
  end

  def assert_eventually(message, fun, timeout_ms \\ 10_000, interval_ms \\ 200)
      when is_binary(message) and is_function(fun, 0) and is_integer(timeout_ms) and
             is_integer(interval_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_assert_eventually(message, fun, deadline, interval_ms)
  end

  def eventually!(message, fun, timeout_ms \\ 10_000, interval_ms \\ 200)
      when is_binary(message) and is_function(fun, 0) and is_integer(timeout_ms) and
             is_integer(interval_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_eventually(message, fun, deadline, interval_ms)
  end

  def wait_until!(deadline, predicate, error_message, interval_ms \\ @default_probe_interval_ms)
      when is_integer(deadline) and is_function(predicate, 0) and is_binary(error_message) do
    cond do
      predicate.() ->
        :ok

      System.monotonic_time(:millisecond) >= deadline ->
        ExUnit.Assertions.flunk(error_message)

      true ->
        Process.sleep(interval_ms)
        wait_until!(deadline, predicate, error_message, interval_ms)
    end
  end

  def seed_client_ready?({host, port}, namespace)
      when is_binary(host) and is_integer(port) and is_binary(namespace) do
    case Tcp.connect(host, port, []) do
      {:ok, conn} ->
        result = Tcp.info(conn, ["partition-generation", "replicas"])
        _ = Tcp.close(conn)
        client_ready_result?(result, namespace)

      {:error, _reason} ->
        false
    end
  end

  def client_ready_result?(
        {:ok, %{"partition-generation" => partition_generation, "replicas" => replicas}},
        namespace
      )
      when is_binary(namespace) and is_binary(replicas) and byte_size(replicas) > 0 do
    case Integer.parse(partition_generation) do
      {generation, ""} when generation >= 0 ->
        String.contains?(replicas, namespace)

      _ ->
        false
    end
  end

  def client_ready_result?(_other, _namespace), do: false

  def cluster_stable?(expected_size, stable_container \\ "aerospike1", opts \\ [])
      when is_integer(expected_size) and is_binary(stable_container) and is_list(opts) do
    ignore_migrations = Keyword.get(opts, :ignore_migrations, false)

    command =
      if ignore_migrations do
        "cluster-stable:size=#{expected_size};ignore-migrations=true"
      else
        "cluster-stable:size=#{expected_size}"
      end

    case System.cmd(
           "docker",
           [
             "exec",
             stable_container,
             "asinfo",
             "-v",
             command
           ],
           stderr_to_stdout: true
         ) do
      {output, 0} ->
        value =
          output
          |> String.trim()
          |> String.split("\t", parts: 2)
          |> List.last()

        is_binary(value) and value != "" and value != "false"

      {_output, _status} ->
        false
    end
  end

  defp do_wait_for_tender_ready(cluster, deadline, interval_ms) do
    :ok = Aerospike.Cluster.Tender.tend_now(cluster)

    cond do
      Aerospike.Cluster.Tender.ready?(cluster) ->
        :ok

      System.monotonic_time(:millisecond) >= deadline ->
        ExUnit.Assertions.flunk(
          "Tender never reached ready? = true within the manual-tend budget"
        )

      true ->
        Process.sleep(interval_ms)
        do_wait_for_tender_ready(cluster, deadline, interval_ms)
    end
  end

  defp do_assert_eventually(message, fun, deadline, interval_ms) do
    cond do
      fun.() ->
        :ok

      System.monotonic_time(:millisecond) >= deadline ->
        ExUnit.Assertions.flunk("condition did not become true within timeout: #{message}")

      true ->
        Process.sleep(interval_ms)
        do_assert_eventually(message, fun, deadline, interval_ms)
    end
  end

  defp do_eventually(message, fun, deadline, interval_ms) do
    case fun.() do
      {:ok, value} ->
        value

      :retry ->
        if System.monotonic_time(:millisecond) >= deadline do
          ExUnit.Assertions.flunk("condition did not become true within timeout: #{message}")
        else
          Process.sleep(interval_ms)
          do_eventually(message, fun, deadline, interval_ms)
        end

      other ->
        raise ArgumentError,
              "eventually!/4 callback must return {:ok, value} or :retry, got: #{inspect(other)}"
    end
  end
end
