defmodule Aerospike.Integration.NodeKillTest do
  @moduledoc """
  Tier 2 exit-criteria integration test — node killed mid-traffic.

  Kills the local `aerospike_spike` docker container while a cluster
  runs against it, then asserts that the Tier 2 substrate behaves as the
  plan requires:

    * the node's lifecycle transitions `:active` → `:inactive` within
      the configured `failure_threshold` tend cycles;
    * in-flight GETs during the outage surface either a transport-class
      `%Aerospike.Error{}` (retry budget exhausted) or the router's
      `:cluster_not_ready` atom once the owners table has been cleared;
    * the node's pool is stopped on the `:active` → `:inactive` flip
      (`Tender.node_handle/2` returns `{:error, :unknown_node}`);
    * `Tender.ready?/1` flips back to `false` and the owners table
      stops routing to the dead node.

  ## Escalation note — recovery is not covered here

  The plan's Task 8 goal includes "the partition map converges within
  one tend cycle when the node comes back" after a `docker start`. The
  Tier 2 implementation — and the existing Task 1 findings in
  `aerospike_driver_spike/docs/plans/tier-2-cluster-correctness/notes.md`
  — make that scenario unreachable with a single-node cluster today:

    * `Aerospike.Tender.bootstrap_if_needed/1` is single-shot
      (`bootstrapped?: true` after the first successful seed dial).
    * After the grace-cycle drop, `state.nodes` is empty, so
      `refresh_nodes/1` has no nodes to probe and
      `discover_peers/1` has no sources to ask for
      `peers-clear-std`.
    * The info socket to the dead node was already closed, so there
      is no surviving channel to promote back to `:active`.

  The practical consequence is that a single-node kill + restart yields
  `{:error, :cluster_not_ready}` indefinitely for this Tender. Recovery
  via re-bootstrap or peer rediscovery is Tier 3 TCP / auth scope (see
  the `notes.md` Task 1 finding). Multi-node topology exercises the
  rebalance / re-route path through surviving nodes, which Tier 2
  covers via the Fake-transport unit tests for Tasks 4 and 7. The
  escalation clause in Task 8's own text — "unit coverage via Fake
  stands as the primary proof; document the gap and note that the full
  scenario needs the multi-node harness deferred to a later plan" —
  applies exactly here.

  This test therefore asserts only the outage-direction of the
  scenario (up to and including the drop) and restarts the container
  in `on_exit/1` so subsequent integration runs still have a live
  server. A full round-trip proof is deferred to the multi-node
  harness plan.
  """

  use ExUnit.Case, async: false

  @moduletag :integration

  alias Aerospike.Error
  alias Aerospike.Key
  alias Aerospike.Tender
  alias Aerospike.Transport.Tcp

  @container "aerospike_spike"
  @host "localhost"
  @port 3000
  @namespace "test"
  @tend_interval_ms 400
  @failure_threshold 3
  @pool_size 2
  # Allow enough tend cycles for the `:active` → `:inactive` transition
  # to land (`@failure_threshold` failing cycles plus slack for the
  # pre-kill tend state machine).
  @inactive_timeout_ms 8_000
  @drop_timeout_ms 4_000
  @probe_interval_ms 100

  setup do
    docker_start(@container)
    probe_aerospike_tcp!(@host, @port)
    wait_for_aerospike_status(@container, 15_000)
    wait_for_client_ready(@host, @port, 15_000)

    name = :"spike_node_kill_cluster_#{System.unique_integer([:positive])}"

    {:ok, sup} =
      Aerospike.start_link(
        name: name,
        transport: Aerospike.Transport.Tcp,
        seeds: [{@host, @port}],
        namespaces: [@namespace],
        tend_interval_ms: @tend_interval_ms,
        failure_threshold: @failure_threshold,
        pool_size: @pool_size,
        # Keep the retry budget small so outage-window GETs return
        # within the wall-clock budget of the test.
        max_retries: 1
      )

    on_exit(fn ->
      try do
        Supervisor.stop(sup)
      catch
        :exit, _ -> :ok
      end

      # Always leave the shared container running — and *serving*
      # `asinfo -v status ok` — so sibling integration tests that
      # rely on the container being up (GetTest / GetPoolTest) see a
      # ready server regardless of file-ordering in the suite.
      docker_start(@container)
      wait_for_tcp(@host, @port, 15_000)
      wait_for_aerospike_status(@container, 15_000)
      wait_for_client_ready(@host, @port, 15_000)
    end)

    wait_for_ready!(name, 10_000)

    %{cluster: name}
  end

  test "container stop mid-traffic demotes the node to :inactive and drops it", %{
    cluster: cluster
  } do
    assert Tender.ready?(cluster),
           "cluster must be ready after start_link + auto-tend"

    [node_name] =
      cluster
      |> Tender.nodes_status()
      |> Map.keys()

    assert %{status: :active, counters: counters_before} =
             Map.fetch!(Tender.nodes_status(cluster), node_name)

    assert is_tuple(counters_before),
           "an :active node must have its NodeCounters reference allocated"

    assert {:ok, _handle} = Tender.node_handle(cluster, node_name)

    # Stop the container. The pool's existing sockets will start
    # returning {:error, %Error{}} on send/recv; the Tender's info
    # socket will fail on its next tend cycle.
    docker_stop(@container)

    outage_results = run_outage_gets(cluster, 6, 800)
    assert outage_results != [], "outage probe must produce at least one result"

    Enum.each(outage_results, fn result ->
      case result do
        {:ok, _record} ->
          flunk(
            "GET unexpectedly succeeded against a stopped container: " <>
              inspect(result)
          )

        {:error, :cluster_not_ready} ->
          :ok

        {:error, :unknown_node} ->
          # Retry exhaustion where every attempt resolved a node via
          # `Router.pick_for_read/4` that `Tender.node_handle/2`
          # immediately refused (node was in-flight towards :inactive
          # when the attempt ran). Same underlying outage signal as a
          # transport error.
          :ok

        {:error, %Error{code: code}}
        when code in [
               :network_error,
               :timeout,
               :connection_error,
               :pool_timeout,
               :invalid_node,
               :circuit_open
             ] ->
          :ok

        other ->
          flunk("unexpected outage GET result: #{inspect(other)}")
      end
    end)

    # The Tender must observe enough failing cycles to flip the node
    # to :inactive. The transition is the single observable Tier 2
    # acceptance criterion: lifecycle moves off :active, the pool is
    # stopped (node_handle rejects), and owners clears for the node.
    assert_status_transition!(cluster, node_name, :inactive, @inactive_timeout_ms)

    assert {:error, :unknown_node} = Tender.node_handle(cluster, node_name),
           "Tender.node_handle must refuse an :inactive node"

    assert {:error, :unknown_node} = Tender.pool_pid(cluster, node_name),
           "Tender.pool_pid must refuse an :inactive node"

    # One more failing cycle drops the node entry entirely (notes.md
    # Task 1: "any failure while :inactive" removes it). After the
    # drop, ready? must reflect the empty topology.
    assert_node_dropped!(cluster, node_name, @drop_timeout_ms)
    refute Tender.ready?(cluster), "ready? must flip to false once all nodes are dropped"
  end

  defp run_outage_gets(cluster, count, timeout_ms) do
    # Small serial burst; we are asserting on classification, not
    # concurrency, and we want the Aerospike.get/3 retry path to run
    # to exhaustion on each call.
    for i <- 1..count do
      key =
        Key.new(
          @namespace,
          "kill_test",
          "outage_#{i}_#{System.unique_integer([:positive])}"
        )

      Aerospike.get(cluster, key, :all, timeout: timeout_ms)
    end
  end

  defp assert_status_transition!(cluster, node_name, expected, timeout_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    wait_for_status(cluster, node_name, expected, deadline)
  end

  defp wait_for_status(cluster, node_name, expected, deadline) do
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
          wait_for_status(cluster, node_name, expected, deadline)
        end
    end
  end

  defp assert_node_dropped!(cluster, node_name, timeout_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    wait_for_drop(cluster, node_name, deadline)
  end

  defp wait_for_drop(cluster, node_name, deadline) do
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
        wait_for_drop(cluster, node_name, deadline)
    end
  end

  defp wait_for_ready!(cluster, timeout_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    wait_for_ready(cluster, deadline)
  end

  defp wait_for_ready(cluster, deadline) do
    cond do
      Tender.ready?(cluster) ->
        :ok

      System.monotonic_time(:millisecond) >= deadline ->
        flunk("cluster never reached ready? = true within the budget")

      true ->
        Process.sleep(@probe_interval_ms)
        wait_for_ready(cluster, deadline)
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

  defp wait_for_aerospike_status(container, timeout_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_wait_for_aerospike_status(container, deadline)
  end

  defp do_wait_for_aerospike_status(container, deadline) do
    case System.cmd("docker", ["exec", container, "asinfo", "-v", "status"],
           stderr_to_stdout: true
         ) do
      {out, 0} ->
        if String.contains?(out, "ok") do
          :ok
        else
          retry_status(container, deadline)
        end

      _ ->
        retry_status(container, deadline)
    end
  end

  defp retry_status(container, deadline) do
    if System.monotonic_time(:millisecond) >= deadline do
      # Teardown paths should not flunk the sibling test that is
      # about to run — leave the next setup to raise a clearer error.
      :error
    else
      Process.sleep(@probe_interval_ms)
      do_wait_for_aerospike_status(container, deadline)
    end
  end

  defp wait_for_client_ready(host, port, timeout_ms) do
    # asinfo -v status = "ok" can transition before the server answers
    # the client-protocol `partition-generation` info call that the
    # Tender issues in its first tend cycle. A sibling integration
    # test that starts its own Tender right after this teardown will
    # observe `Tender.ready?/1 == false` if the cycle races the
    # server warm-up. Driving a real info request through
    # `Aerospike.Transport.Tcp` is the cheapest way to prove the
    # server is fully serving the client protocol before this
    # teardown returns.
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_wait_for_client_ready(host, port, deadline)
  end

  defp do_wait_for_client_ready(host, port, deadline) do
    # Drive a full tend-cycle info fetch through a fresh connection:
    # `partition-generation` proves the cluster-state subsystem is up,
    # `replicas` proves the partition map is computed, and a non-empty
    # `replicas` reply covering the `@namespace` configured here is
    # the exact precondition `Tender.ready?/1` needs.
    case Tcp.connect(host, port, []) do
      {:ok, conn} ->
        result = Tcp.info(conn, ["partition-generation", "replicas"])
        _ = Tcp.close(conn)
        handle_client_ready_result(result, host, port, deadline)

      {:error, _} ->
        retry_client_ready(host, port, deadline)
    end
  end

  defp handle_client_ready_result(
         {:ok, %{"partition-generation" => gen, "replicas" => replicas}},
         host,
         port,
         deadline
       )
       when is_binary(replicas) and byte_size(replicas) > 0 do
    if partition_generation_settled?(gen) and String.contains?(replicas, @namespace),
      do: :ok,
      else: retry_client_ready(host, port, deadline)
  end

  defp handle_client_ready_result(_other, host, port, deadline) do
    retry_client_ready(host, port, deadline)
  end

  # Immediately after a container restart the server advertises
  # `partition-generation = -1` until the partition map has been
  # finalised. Waiting for a non-negative value is the cheapest
  # signal that a subsequent Tender's first tend cycle will install
  # a non-empty owners table — which is the precondition
  # `Tender.ready?/1` actually reads.
  defp partition_generation_settled?(gen) when is_binary(gen) do
    case Integer.parse(gen) do
      {n, _rest} when n >= 0 -> true
      _ -> false
    end
  end

  defp partition_generation_settled?(_), do: false

  defp retry_client_ready(host, port, deadline) do
    if System.monotonic_time(:millisecond) >= deadline do
      :error
    else
      Process.sleep(@probe_interval_ms)
      do_wait_for_client_ready(host, port, deadline)
    end
  end

  defp wait_for_tcp(host, port, timeout_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_wait_for_tcp(host, port, deadline)
  end

  defp do_wait_for_tcp(host, port, deadline) do
    case :gen_tcp.connect(to_charlist(host), port, [:binary, active: false], 500) do
      {:ok, sock} ->
        :gen_tcp.close(sock)
        :ok

      {:error, _} ->
        if System.monotonic_time(:millisecond) >= deadline do
          # Let the next setup block raise a clearer error — do not
          # flunk on teardown paths.
          :error
        else
          Process.sleep(@probe_interval_ms)
          do_wait_for_tcp(host, port, deadline)
        end
    end
  end

  defp probe_aerospike_tcp!(host, port) do
    case wait_for_tcp(host, port, 10_000) do
      :ok ->
        :ok

      :error ->
        raise "Aerospike not reachable at #{host}:#{port}. " <>
                "Run `docker compose up -d` first."
    end
  end
end
