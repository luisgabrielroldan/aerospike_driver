defmodule Aerospike.Integration.OperatorSurfaceSmokeTest do
  @moduledoc """
  End-to-end smoke test that proves TLS, session-login, and the
  telemetry taxonomy all co-operate through the real transport path
  against a live Enterprise Edition container.

  The container (`aerospike-ee-security-tls`) has *both* security
  enabled and a TLS listener (port 4433). This is the only profile
  that exercises TLS and auth simultaneously against a single
  transport handshake — the other EE profiles are either TLS-only
  (`aerospike-ee-tls`, 4333) or security-only (`aerospike-ee-security`,
  3200). Start it with
  `docker compose --profile enterprise up -d aerospike-ee-security-tls`.

  The scenario:

    1. open an `Aerospike.Transport.Tls` cluster with a `spike` data
       user, client cert trust, and auto-tend enabled;
    2. attach a `:telemetry` handler to every event the driver emits
       (pool checkout, command send/recv, info RPC, tend cycle,
       partition-map refresh, node transition, retry attempt);
    3. perform 100 serial GETs against missing keys and assert that
       the pool-checkout / command / info / tend spans all fired, and
       that a `:bootstrap` node transition landed during bring-up;
    4. exercise the kill/recovery proof through the secured TLS
       transport: stop the container, observe the node flipping to
       `:inactive` and getting dropped (`:failure_threshold` +
       `:dropped` transitions), restart it, assert recovery and that
       fresh GETs succeed.

  Tagged `:integration` and `:enterprise` so it is excluded from the
  default suite; run with
  `mix test --include integration --include enterprise`.
  """

  use ExUnit.Case, async: false

  @moduletag :integration
  @moduletag :enterprise

  alias Aerospike.Cluster
  alias Aerospike.Cluster.Tender
  alias Aerospike.Error
  alias Aerospike.Key
  alias Aerospike.Telemetry

  @container "aerospike-ee-security-tls"
  @host "localhost"
  @port 4433
  # Plain client port exposed by the same container, used only for the
  # in-test readiness probe that confirms the server is serving the
  # client protocol before we drive the Tender. `asinfo` over plain TCP
  # against a security-enabled server rejects unauthenticated commands,
  # so we use a simple TCP `connect` as the probe.
  @plain_port 3300
  @namespace "test"
  @data_user "spike"
  @data_password "spike-password"

  @fixtures_dir "test/support/fixtures/tls"
  @ca_cert Path.join(@fixtures_dir, "ca.crt")

  @tend_interval_ms 400
  @failure_threshold 3
  @pool_size 2
  @probe_interval_ms 100
  @ready_timeout_ms 10_000
  @inactive_timeout_ms 8_000
  @drop_timeout_ms 4_000
  @recovery_timeout_ms 12_000

  setup_all do
    probe_tcp!(@host, @plain_port)
    ensure_data_user!(@data_user, @data_password)
    :ok
  end

  setup do
    docker_start(@container)
    wait_for_tcp(@host, @plain_port, 15_000)
    wait_for_aerospike_status_authed(@container, 15_000)

    test_pid = self()
    ref = make_ref()

    handler_id = {__MODULE__, ref, :events}

    events = Telemetry.handler_events()

    :ok =
      :telemetry.attach_many(
        handler_id,
        events,
        &__MODULE__.forward/4,
        %{pid: test_pid, ref: ref}
      )

    on_exit(fn ->
      :telemetry.detach(handler_id)
      docker_start(@container)
      wait_for_tcp(@host, @plain_port, 15_000)
      wait_for_aerospike_status_authed(@container, 15_000)
    end)

    %{handler_ref: ref}
  end

  test "TLS + session-login + telemetry + kill/recovery end-to-end", %{
    handler_ref: ref
  } do
    cluster = start_cluster!()
    wait_for_ready!(cluster, @ready_timeout_ms)

    refute Aerospike.metrics_enabled?(cluster)
    assert :ok = Aerospike.enable_metrics(cluster, reset: true)
    assert Aerospike.metrics_enabled?(cluster)

    assert %{
             metrics_enabled: true,
             cluster: %{config: %{pool_size: @pool_size}}
           } = Aerospike.stats(cluster)

    [node_name] = Cluster.active_nodes(cluster)

    assert {:ok,
            %{
              status: :ok,
              requested_per_node: @pool_size,
              total_requested: @pool_size,
              total_warmed: @pool_size,
              nodes_total: 1,
              nodes_ok: 1,
              nodes_partial: 0,
              nodes_error: 0
            }} = Aerospike.warm_up(cluster)

    # 100 serial GETs against missing keys exercise the pool-checkout
    # span, command send/recv spans, and keep the tend cycle ticking
    # in the background.
    for i <- 1..100 do
      key =
        Key.new(
          @namespace,
          "operator_surface_smoke",
          "missing_#{i}_#{System.unique_integer([:positive])}"
        )

      assert {:error, %Error{code: :key_not_found}} =
               Aerospike.get(cluster, key, :all, timeout: 2_000)
    end

    # Give the tender another cycle to ensure at least one full
    # tend_cycle + partition_map_refresh span has been observed.
    :ok = Tender.tend_now(cluster)

    counts = drain_and_count(ref)

    # Pool checkout must have fired at least once per GET (100 stops).
    assert counts[pool_start()] >= 100,
           "expected >=100 pool-checkout :start events, got " <>
             inspect(counts[pool_start()])

    assert counts[pool_stop()] >= 100,
           "expected >=100 pool-checkout :stop events, got " <>
             inspect(counts[pool_stop()])

    # Every GET completes a full command send + recv bracket.
    assert counts[cmd_send_start()] >= 100
    assert counts[cmd_send_stop()] >= 100
    assert counts[cmd_recv_start()] >= 100
    assert counts[cmd_recv_stop()] >= 100

    # Info RPCs fire from the Tender (partition-generation / replicas)
    # and from the login handshake at worker init. Both must have
    # landed on a ready cluster.
    assert counts[info_start()] >= 1
    assert counts[info_stop()] >= 1

    # Tend cycle span + nested partition-map-refresh span. `tend_now/1`
    # above guarantees at least one.
    assert counts[tend_start()] >= 1
    assert counts[tend_stop()] >= 1
    assert counts[pmap_start()] >= 1
    assert counts[pmap_stop()] >= 1

    # Bootstrap transition landed during cluster bring-up.
    assert counts[transition()] >= 1

    # Kill/recovery path through the secured TLS transport.
    docker_stop(@container)

    # Drain events produced while the container was still alive so the
    # post-kill assertions see only outage-era events.
    _ = drain_and_count(ref)

    assert_status_transition!(cluster, node_name, :inactive, @inactive_timeout_ms)
    assert_node_dropped!(cluster, node_name, @drop_timeout_ms)
    refute Cluster.ready?(cluster), "ready? must flip to false once all nodes are dropped"

    outage_counts = drain_and_count(ref)

    failure_threshold_transitions =
      Enum.count(Map.get(outage_counts, :transitions, []), fn meta ->
        meta[:to] == :inactive and meta[:reason] == :failure_threshold
      end)

    dropped_transitions =
      Enum.count(Map.get(outage_counts, :transitions, []), fn meta ->
        meta[:to] == :unknown and meta[:reason] == :dropped
      end)

    assert failure_threshold_transitions >= 1,
           "expected a :failure_threshold transition during the outage, " <>
             "got transitions=" <> inspect(Map.get(outage_counts, :transitions, []))

    assert dropped_transitions >= 1,
           "expected a :dropped transition after the failure threshold cycles, " <>
             "got transitions=" <> inspect(Map.get(outage_counts, :transitions, []))

    # Restart the container and assert full recovery.
    docker_start(@container)
    wait_for_tcp(@host, @plain_port, 15_000)
    wait_for_aerospike_status_authed(@container, 15_000)

    assert_recovered!(cluster, @recovery_timeout_ms)

    key =
      Key.new(
        @namespace,
        "operator_surface_smoke",
        "recovered_#{System.unique_integer([:positive])}"
      )

    assert {:error, %Error{code: :key_not_found}} =
             Aerospike.get(cluster, key, :all, timeout: 2_000)

    stats = Aerospike.stats(cluster)
    assert stats.commands_total >= 101
    assert stats.commands_error >= 101
  end

  # --- helpers -----------------------------------------------------------

  @doc false
  def forward(event, measurements, metadata, %{pid: pid, ref: ref}) do
    send(pid, {:operator_surface_event, ref, event, measurements, metadata})
  end

  # Drain every event currently sitting in the mailbox and return the
  # accumulated counts. Uses a zero-timeout receive so the helper does
  # not block waiting for new events — tend cycles keep firing while
  # the test runs, so a "quiet" window never actually arrives.
  defp drain_and_count(ref) do
    do_drain(ref, %{transitions: []})
  end

  defp do_drain(ref, acc) do
    receive do
      {:operator_surface_event, ^ref, event, _measurements, metadata} ->
        acc =
          acc
          |> Map.update(event, 1, &(&1 + 1))
          |> maybe_record_transition(event, metadata)

        do_drain(ref, acc)
    after
      0 -> acc
    end
  end

  defp maybe_record_transition(acc, event, metadata) do
    if event == Telemetry.node_transition() do
      Map.update!(acc, :transitions, fn list -> [metadata | list] end)
    else
      acc
    end
  end

  defp start_cluster! do
    name = :"spike_operator_surface_smoke_#{System.unique_integer([:positive])}"

    {:ok, sup} =
      Aerospike.start_link(
        name: name,
        transport: Aerospike.Transport.Tls,
        hosts: ["#{@host}:#{@port}"],
        namespaces: [@namespace],
        tend_interval_ms: @tend_interval_ms,
        failure_threshold: @failure_threshold,
        pool_size: @pool_size,
        max_retries: 1,
        user: @data_user,
        password: @data_password,
        connect_opts: [
          tls_name: "localhost",
          tls_cacertfile: @ca_cert
        ]
      )

    on_exit(fn ->
      try do
        Supervisor.stop(sup)
      catch
        :exit, _ -> :ok
      end
    end)

    name
  end

  defp wait_for_ready!(cluster, timeout_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_wait_ready(cluster, deadline)
  end

  defp do_wait_ready(cluster, deadline) do
    cond do
      Cluster.ready?(cluster) ->
        :ok

      System.monotonic_time(:millisecond) >= deadline ->
        flunk("cluster never reached ready? = true within the budget")

      true ->
        Process.sleep(@probe_interval_ms)
        do_wait_ready(cluster, deadline)
    end
  end

  defp assert_status_transition!(cluster, node_name, expected, timeout_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_wait_status(cluster, node_name, expected, deadline)
  end

  defp do_wait_status(cluster, node_name, expected, deadline) do
    case Tender.nodes_status(cluster) do
      %{^node_name => %{status: ^expected}} ->
        :ok

      status ->
        if System.monotonic_time(:millisecond) >= deadline do
          flunk(
            "node #{node_name} never reached :#{expected} within the budget " <>
              "(last nodes_status=#{inspect(status)})"
          )
        else
          Process.sleep(@probe_interval_ms)
          do_wait_status(cluster, node_name, expected, deadline)
        end
    end
  end

  defp assert_node_dropped!(cluster, node_name, timeout_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_wait_drop(cluster, node_name, deadline)
  end

  defp do_wait_drop(cluster, node_name, deadline) do
    status = Tender.nodes_status(cluster)

    cond do
      not Map.has_key?(status, node_name) ->
        :ok

      System.monotonic_time(:millisecond) >= deadline ->
        flunk(
          "node #{node_name} was not dropped within the budget " <>
            "(last nodes_status=#{inspect(status)})"
        )

      true ->
        Process.sleep(@probe_interval_ms)
        do_wait_drop(cluster, node_name, deadline)
    end
  end

  defp assert_recovered!(cluster, timeout_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_wait_recovery(cluster, deadline)
  end

  defp do_wait_recovery(cluster, deadline) do
    status = Tender.nodes_status(cluster)

    recovered? =
      Cluster.ready?(cluster) and
        map_size(status) > 0 and
        Enum.all?(status, fn {_name, node} ->
          node.status == :active and is_integer(node.generation_seen) and
            node.generation_seen >= 0
        end)

    cond do
      recovered? ->
        :ok

      System.monotonic_time(:millisecond) >= deadline ->
        flunk(
          "cluster did not recover within the budget " <>
            "(last nodes_status=#{inspect(status)}, ready?=#{inspect(Cluster.ready?(cluster))})"
        )

      true ->
        Process.sleep(@probe_interval_ms)
        do_wait_recovery(cluster, deadline)
    end
  end

  defp docker_stop(container) do
    {_, 0} = System.cmd("docker", ["stop", container], stderr_to_stdout: true)
    :ok
  end

  defp docker_start(container) do
    case System.cmd("docker", ["start", container], stderr_to_stdout: true) do
      {_, 0} ->
        :ok

      {out, code} ->
        flunk("docker start #{container} failed (exit=#{code}): #{out}")
    end
  end

  defp wait_for_tcp(host, port, timeout_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_wait_tcp(host, port, deadline)
  end

  defp do_wait_tcp(host, port, deadline) do
    case :gen_tcp.connect(to_charlist(host), port, [:binary, active: false], 500) do
      {:ok, sock} ->
        :gen_tcp.close(sock)
        :ok

      {:error, _} ->
        if System.monotonic_time(:millisecond) >= deadline do
          :error
        else
          Process.sleep(@probe_interval_ms)
          do_wait_tcp(host, port, deadline)
        end
    end
  end

  defp wait_for_aerospike_status_authed(container, timeout_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_wait_authed_status(container, deadline)
  end

  defp do_wait_authed_status(container, deadline) do
    case System.cmd(
           "docker",
           ["exec", container, "asinfo", "-U", "admin", "-P", "admin", "-v", "status"],
           stderr_to_stdout: true
         ) do
      {out, 0} ->
        if String.contains?(out, "ok") do
          :ok
        else
          retry_authed_status(container, deadline)
        end

      _ ->
        retry_authed_status(container, deadline)
    end
  end

  defp retry_authed_status(container, deadline) do
    if System.monotonic_time(:millisecond) >= deadline do
      :error
    else
      Process.sleep(@probe_interval_ms)
      do_wait_authed_status(container, deadline)
    end
  end

  defp probe_tcp!(host, port) do
    case wait_for_tcp(host, port, 10_000) do
      :ok ->
        :ok

      :error ->
        raise "Aerospike EE security+TLS profile not reachable at #{host}:#{port}. " <>
                "Run `docker compose --profile enterprise up -d aerospike-ee-security-tls` first."
    end
  end

  defp ensure_data_user!(user, password) do
    case asadm(["manage acl create user #{user} password #{password} roles read-write"]) do
      {_out, 0} ->
        :ok

      {out, _} ->
        if out =~ "already exists" do
          :ok
        else
          raise "Failed to provision data user #{user}: #{out}"
        end
    end
  end

  defp asadm(commands) do
    args = Enum.flat_map(commands, fn cmd -> ["-e", cmd] end)

    System.cmd(
      "docker",
      ["exec", @container, "asadm", "-U", "admin", "-P", "admin", "--enable"] ++ args,
      stderr_to_stdout: true
    )
  end

  # Compile-time event-name helpers so assertions read naturally and
  # can't drift from the telemetry taxonomy.
  defp pool_start, do: Telemetry.pool_checkout_span() ++ [:start]
  defp pool_stop, do: Telemetry.pool_checkout_span() ++ [:stop]
  defp cmd_send_start, do: Telemetry.command_send_span() ++ [:start]
  defp cmd_send_stop, do: Telemetry.command_send_span() ++ [:stop]
  defp cmd_recv_start, do: Telemetry.command_recv_span() ++ [:start]
  defp cmd_recv_stop, do: Telemetry.command_recv_span() ++ [:stop]
  defp info_start, do: Telemetry.info_rpc_span() ++ [:start]
  defp info_stop, do: Telemetry.info_rpc_span() ++ [:stop]
  defp tend_start, do: Telemetry.tend_cycle_span() ++ [:start]
  defp tend_stop, do: Telemetry.tend_cycle_span() ++ [:stop]
  defp pmap_start, do: Telemetry.partition_map_refresh_span() ++ [:start]
  defp pmap_stop, do: Telemetry.partition_map_refresh_span() ++ [:stop]
  defp transition, do: Telemetry.node_transition()
end
