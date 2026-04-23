defmodule Aerospike.Integration.AdminTruncateTest do
  use ExUnit.Case, async: false

  @moduletag :integration

  alias Aerospike.Cluster.Tender
  alias Aerospike.Error
  alias Aerospike.Key

  @host "localhost"
  @port 3000
  @namespace "test"

  setup do
    probe_aerospike!(@host, @port)
    name = :"spike_admin_truncate_cluster_#{System.unique_integer([:positive])}"

    {:ok, sup} =
      Aerospike.start_link(
        name: name,
        transport: Aerospike.Transport.Tcp,
        hosts: ["#{@host}:#{@port}"],
        namespaces: [@namespace],
        tend_trigger: :manual,
        pool_size: 2
      )

    :ok = Tender.tend_now(name)

    on_exit(fn ->
      try do
        Supervisor.stop(sup)
      catch
        :exit, _ -> :ok
      end
    end)

    %{cluster: name}
  end

  test "truncate/3 respects the optional before cutoff", %{cluster: cluster} do
    older_set = "truncate_ns_#{System.unique_integer([:positive, :monotonic])}"
    newer_set = "#{older_set}_new"
    older_key = Key.new(@namespace, older_set, "older")
    newer_key = Key.new(@namespace, newer_set, "newer")

    assert {:ok, _metadata} = Aerospike.put(cluster, older_key, %{"v" => 1})
    Process.sleep(20)
    before = DateTime.utc_now()
    Process.sleep(20)
    assert {:ok, _metadata} = Aerospike.put(cluster, newer_key, %{"v" => 2})

    assert :ok = Aerospike.truncate(cluster, @namespace, before: before)

    assert_eventually(
      fn ->
        older_state = Aerospike.get(cluster, older_key)
        newer_state = Aerospike.get(cluster, newer_key)

        condition? =
          match?({:error, %Error{code: :key_not_found}}, older_state) and
            match?({:ok, _}, newer_state)

        {condition?,
         "namespace truncate cutoff mismatch: older=#{inspect(older_state)} newer=#{inspect(newer_state)}"}
      end,
      timeout: truncate_wait_timeout_ms(),
      interval: 200
    )
  end

  test "truncate/4 truncates only the targeted set", %{cluster: cluster} do
    set = "truncate_set_#{System.unique_integer([:positive, :monotonic])}"
    other_set = "#{set}_keep"
    trunc_key = Key.new(@namespace, set, "trunc")
    keep_key = Key.new(@namespace, other_set, "keep")

    assert {:ok, _metadata} = Aerospike.put(cluster, trunc_key, %{"v" => 1})
    assert {:ok, _metadata} = Aerospike.put(cluster, keep_key, %{"v" => 2})

    assert :ok = Aerospike.truncate(cluster, @namespace, set)

    assert_eventually(
      fn ->
        trunc_state = Aerospike.get(cluster, trunc_key)
        keep_state = Aerospike.get(cluster, keep_key)

        condition? =
          match?({:error, %Error{code: :key_not_found}}, trunc_state) and
            match?({:ok, _}, keep_state)

        {condition?,
         "set truncate mismatch: trunc=#{inspect(trunc_state)} keep=#{inspect(keep_state)}"}
      end,
      timeout: truncate_wait_timeout_ms(),
      interval: 200
    )
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

  defp truncate_wait_timeout_ms, do: 15_000

  defp probe_aerospike!(host, port) do
    case :gen_tcp.connect(to_charlist(host), port, [:binary, active: false], 1_000) do
      {:ok, sock} ->
        :gen_tcp.close(sock)
        :ok

      {:error, reason} ->
        raise "Aerospike not reachable at #{host}:#{port} (#{inspect(reason)}). " <>
                "Run `docker compose up -d` first."
    end
  end
end
