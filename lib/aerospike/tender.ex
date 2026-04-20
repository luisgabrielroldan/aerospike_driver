defmodule Aerospike.Tender do
  @moduledoc """
  Single-writer cluster-state GenServer.

  The Tender is the only process that writes to the cluster's ETS tables
  (partition owners, per-node partition-generation, meta/ready flag). Every
  other component reads directly from ETS for a lock-free hot path.

  Responsibilities:

    * Bootstrap the node set from a seed list.
    * Periodically discover peers from every known node and add any that
      show up.
    * Track each node's reported `partition-generation`; refetch the
      partition map for nodes whose generation advanced.
    * Apply the regime guard on every partition-map update so a late reply
      from a lagging node cannot overwrite a newer one.
    * Count consecutive refresh failures per node; drop a node only when
      the failure threshold is reached.
    * Flip the `:ready` meta flag only once every configured namespace has
      a complete partition map (every partition 0..4095 has a master).

  All I/O goes through the `Aerospike.NodeTransport` behaviour module
  supplied via the `:transport` option. Production code uses
  `Aerospike.Transport.Tcp`; tests substitute `Aerospike.Transport.Fake`
  with scripted replies.

  Tests drive the cycle deterministically with `tend_now/1`, which blocks
  until the cycle's ETS writes are visible.
  """

  use GenServer

  require Logger

  alias Aerospike.Error
  alias Aerospike.NodeSupervisor
  alias Aerospike.PartitionMap
  alias Aerospike.Protocol.PartitionMap, as: PartitionMapParser
  alias Aerospike.Protocol.Peers
  alias Aerospike.TableOwner

  @default_tend_interval_ms 1_000
  @default_failure_threshold 5
  @default_pool_size 10

  @type seed :: {String.t(), :inet.port_number()}
  @type namespace :: String.t()

  @typedoc """
  Start options:

    * `:name` — registered name (required).
    * `:transport` — module implementing `Aerospike.NodeTransport` (required).
    * `:seeds` — list of `{host, port}` tuples (required, non-empty).
    * `:namespaces` — list of configured namespaces the cluster must serve
      before `:ready` flips to `true` (required, non-empty).
    * `:tables` — `%{owners: atom(), node_gens: atom(), meta: atom()}`
      map of ETS table names, as returned by `Aerospike.TableOwner.tables/1`
      (required). The Tender does not create tables; it reads and writes
      the tables created by the TableOwner so the partition map survives
      a Tender restart.
    * `:connect_opts` — keyword forwarded verbatim to
      `transport.connect/3`. Default `[]`.
    * `:failure_threshold` — consecutive refresh failures before a node is
      dropped. Default `#{@default_failure_threshold}`.
    * `:tend_interval_ms` — period between automatic tend cycles.
      Default `#{@default_tend_interval_ms}`.
    * `:tend_trigger` — `:timer` (default) or `:manual`. In `:manual`
      mode no timer is started and tests drive cycles with `tend_now/1`.
    * `:node_supervisor` — registered name (atom) or pid of the
      `Aerospike.NodeSupervisor` used to start per-node connection
      pools. When absent, pool lifecycle is skipped entirely — the
      Tender runs with info sockets only, which is the mode the
      cluster-state invariant tests rely on.
    * `:pool_size` — pool workers per node. Default
      `#{@default_pool_size}`. Ignored when `:node_supervisor` is
      absent.
  """
  @type option ::
          {:name, GenServer.name()}
          | {:transport, module()}
          | {:seeds, [seed(), ...]}
          | {:namespaces, [namespace(), ...]}
          | {:tables, TableOwner.tables()}
          | {:connect_opts, keyword()}
          | {:failure_threshold, pos_integer()}
          | {:tend_interval_ms, pos_integer()}
          | {:tend_trigger, :timer | :manual}
          | {:node_supervisor, atom() | pid()}
          | {:pool_size, pos_integer()}

  @spec start_link([option()]) :: GenServer.on_start()
  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Runs one tend cycle synchronously. Returns once every ETS write from
  the cycle has been committed.
  """
  @spec tend_now(GenServer.server()) :: :ok
  def tend_now(server) do
    GenServer.call(server, :tend_now, 30_000)
  end

  @doc """
  Returns whether every configured namespace has a complete partition map.
  """
  @spec ready?(GenServer.server()) :: boolean()
  def ready?(server) do
    GenServer.call(server, :ready?)
  end

  @doc """
  Returns the names of the Tender's ETS tables, intended for Router reads.

  The `:meta` table holds lock-free cluster state flags (currently only
  `:ready`) so hot-path readers can consult cluster state without a
  `GenServer.call` into the Tender.
  """
  @spec tables(GenServer.server()) :: %{owners: atom(), node_gens: atom(), meta: atom()}
  def tables(server) do
    GenServer.call(server, :tables)
  end

  @doc """
  Returns the pid of the `Aerospike.NodePool` serving `node_name`.

  Returns `{:error, :unknown_node}` if the node is not in the cluster's
  current view or if the Tender was started without a `:node_supervisor`
  (i.e. cluster-state-only mode used by some tests). Command modules
  (Task 8 onward) use this to check out a pooled connection without
  re-opening a socket per call.
  """
  @spec pool_pid(GenServer.server(), String.t()) ::
          {:ok, pid()} | {:error, :unknown_node}
  def pool_pid(server, node_name) when is_binary(node_name) do
    GenServer.call(server, {:pool_pid, node_name})
  end

  @doc """
  Returns the transport module this Tender dispatches I/O through.

  Command modules pair this with `pool_pid/2`: checkout a pool worker,
  then call `transport.command/2` on the checked-out connection. The
  value is fixed at `start_link/1` time and does not change across
  tend cycles.
  """
  @spec transport(GenServer.server()) :: module()
  def transport(server) do
    GenServer.call(server, :transport)
  end

  @impl GenServer
  def init(opts) do
    transport = Keyword.fetch!(opts, :transport)
    seeds = Keyword.fetch!(opts, :seeds)
    namespaces = Keyword.fetch!(opts, :namespaces)
    tables = Keyword.fetch!(opts, :tables)

    seeds != [] or raise ArgumentError, "Aerospike.Tender: :seeds must be non-empty"

    namespaces != [] or
      raise ArgumentError, "Aerospike.Tender: :namespaces must be non-empty"

    %{owners: owners_tab, node_gens: node_gens_tab, meta: meta_tab} = tables

    state = %{
      transport: transport,
      connect_opts: Keyword.get(opts, :connect_opts, []),
      seeds: seeds,
      namespaces: namespaces,
      failure_threshold: Keyword.get(opts, :failure_threshold, @default_failure_threshold),
      tend_interval_ms: Keyword.get(opts, :tend_interval_ms, @default_tend_interval_ms),
      tend_trigger: Keyword.get(opts, :tend_trigger, :timer),
      node_supervisor: Keyword.get(opts, :node_supervisor),
      pool_size: Keyword.get(opts, :pool_size, @default_pool_size),
      owners_tab: owners_tab,
      node_gens_tab: node_gens_tab,
      meta_tab: meta_tab,
      nodes: %{},
      ready?: read_ready(meta_tab),
      bootstrapped?: false
    }

    cleanup_orphan_pools(state)

    {:ok, maybe_schedule_tend(state)}
  end

  @impl GenServer
  def handle_call(:tend_now, _from, state) do
    {:reply, :ok, run_tend(state)}
  end

  def handle_call(:ready?, _from, state) do
    {:reply, state.ready?, state}
  end

  def handle_call(:tables, _from, state) do
    {:reply, %{owners: state.owners_tab, node_gens: state.node_gens_tab, meta: state.meta_tab},
     state}
  end

  def handle_call({:pool_pid, node_name}, _from, state) do
    case Map.fetch(state.nodes, node_name) do
      {:ok, %{pool_pid: pid}} when is_pid(pid) -> {:reply, {:ok, pid}, state}
      _ -> {:reply, {:error, :unknown_node}, state}
    end
  end

  def handle_call(:transport, _from, state) do
    {:reply, state.transport, state}
  end

  @impl GenServer
  def handle_info(:tend, state) do
    state = run_tend(state)
    {:noreply, maybe_schedule_tend(state)}
  end

  @impl GenServer
  def terminate(_reason, state) do
    Enum.each(state.nodes, fn {_, node} -> safe_close(state.transport, node.conn) end)
    :ok
  end

  ## Tend cycle

  defp run_tend(state) do
    state
    |> bootstrap_if_needed()
    |> refresh_nodes()
    |> discover_peers()
    |> refresh_partition_maps()
    |> recompute_ready()
  end

  defp bootstrap_if_needed(%{bootstrapped?: true} = state), do: state

  defp bootstrap_if_needed(state) do
    state =
      Enum.reduce(state.seeds, state, fn {host, port}, acc ->
        bootstrap_seed(acc, host, port)
      end)

    if map_size(state.nodes) > 0 do
      %{state | bootstrapped?: true}
    else
      state
    end
  end

  defp bootstrap_seed(state, host, port) do
    with {:ok, conn} <- state.transport.connect(host, port, state.connect_opts),
         {:ok, %{"node" => node_name}} <- state.transport.info(conn, ["node"]) do
      case Map.fetch(state.nodes, node_name) do
        {:ok, _existing} ->
          safe_close(state.transport, conn)
          state

        :error ->
          register_new_node(state, node_name, host, port, conn)
      end
    else
      {:error, %Error{} = err} ->
        Logger.warning("Aerospike.Tender: seed #{host}:#{port} bootstrap failed: #{err.message}")
        state

      {:ok, _other} ->
        Logger.warning("Aerospike.Tender: seed #{host}:#{port} missing 'node' info")
        state
    end
  end

  defp register_new_node(state, node_name, host, port, conn) do
    case ensure_pool(state, node_name, host, port) do
      {:ok, pool_pid} ->
        node = %{
          name: node_name,
          host: host,
          port: port,
          conn: conn,
          failures: 0,
          pool_pid: pool_pid
        }

        %{state | nodes: Map.put(state.nodes, node_name, node)}

      :error ->
        safe_close(state.transport, conn)
        state
    end
  end

  defp refresh_nodes(state) do
    Enum.reduce(state.nodes, state, fn {name, _}, acc ->
      case Map.fetch(acc.nodes, name) do
        {:ok, node} -> refresh_node(acc, node)
        :error -> acc
      end
    end)
  end

  defp refresh_node(state, node) do
    case state.transport.info(node.conn, ["partition-generation"]) do
      {:ok, info} ->
        handle_partition_generation(state, node, info)

      {:error, %Error{} = err} ->
        Logger.debug("Aerospike.Tender: #{node.name} partition-generation failed: #{err.message}")
        register_failure(state, node.name)
    end
  end

  defp handle_partition_generation(state, node, %{"partition-generation" => value}) do
    case PartitionMapParser.parse_partition_generation(value) do
      {:ok, gen} ->
        store_node_gen_if_changed(state, node.name, gen)
        update_node(state, node.name, fn n -> %{n | failures: 0} end)

      :error ->
        register_failure(state, node.name)
    end
  end

  defp handle_partition_generation(state, node, _info) do
    register_failure(state, node.name)
  end

  defp store_node_gen_if_changed(state, name, gen) do
    case PartitionMap.get_node_gen(state.node_gens_tab, name) do
      {:ok, ^gen} -> :ok
      _ -> PartitionMap.put_node_gen(state.node_gens_tab, name, gen)
    end
  end

  defp register_failure(state, name) do
    state =
      update_node(state, name, fn node -> %{node | failures: node.failures + 1} end)

    case Map.fetch(state.nodes, name) do
      {:ok, %{failures: failures}} when failures >= state.failure_threshold ->
        drop_node(state, name)

      _ ->
        state
    end
  end

  defp drop_node(state, name) do
    case Map.pop(state.nodes, name) do
      {nil, _} ->
        state

      {node, nodes} ->
        drop_pool(state, node)
        safe_close(state.transport, node.conn)
        PartitionMap.delete_node_gen(state.node_gens_tab, name)
        PartitionMap.drop_node(state.owners_tab, name)
        %{state | nodes: nodes}
    end
  end

  defp update_node(state, name, fun) do
    case Map.fetch(state.nodes, name) do
      {:ok, node} -> %{state | nodes: Map.put(state.nodes, name, fun.(node))}
      :error -> state
    end
  end

  ## Peer discovery

  # Iterates over every known node (ordered by node name) asking for
  # peers-clear-std. Every parseable reply contributes to the peer set —
  # we do not stop at the first one. This avoids the "first-by-term-order"
  # failure mode where a stale or broken lead node masks real topology.
  defp discover_peers(state) do
    ordered = sorted_nodes(state.nodes)

    {state, peers_by_name} =
      Enum.reduce(ordered, {state, %{}}, &collect_peers/2)

    Enum.reduce(peers_by_name, state, fn {_name, peer}, acc ->
      ensure_peer_connected(acc, peer)
    end)
  end

  defp collect_peers(node, {state, peers_acc}) do
    case state.transport.info(node.conn, ["peers-clear-std"]) do
      {:ok, %{"peers-clear-std" => value}} ->
        handle_peers_value(state, node, value, peers_acc)

      {:ok, _other} ->
        {register_failure(state, node.name), peers_acc}

      {:error, %Error{} = err} ->
        Logger.debug("Aerospike.Tender: #{node.name} peers-clear-std failed: #{err.message}")
        {register_failure(state, node.name), peers_acc}
    end
  end

  defp handle_peers_value(state, node, value, peers_acc) do
    case Peers.parse_peers_clear_std(value) do
      {:ok, %{peers: peers}} ->
        state = update_node(state, node.name, fn n -> %{n | failures: 0} end)
        {state, merge_peers(peers_acc, peers)}

      :error ->
        {register_failure(state, node.name), peers_acc}
    end
  end

  defp merge_peers(acc, peers) do
    Enum.reduce(peers, acc, fn peer, m -> Map.put_new(m, peer.node_name, peer) end)
  end

  defp ensure_peer_connected(state, %{node_name: name, host: host, port: port}) do
    case Map.has_key?(state.nodes, name) do
      true ->
        state

      false ->
        case state.transport.connect(host, port, state.connect_opts) do
          {:ok, conn} ->
            register_new_node(state, name, host, port, conn)

          {:error, %Error{} = err} ->
            Logger.warning(
              "Aerospike.Tender: could not connect to peer #{name} at #{host}:#{port}: #{err.message}"
            )

            state
        end
    end
  end

  ## Partition map refresh

  defp refresh_partition_maps(state) do
    state.nodes
    |> sorted_nodes()
    |> Enum.reduce(state, fn node, acc ->
      case Map.fetch(acc.nodes, node.name) do
        {:ok, current} -> maybe_refresh_partition_map(acc, current)
        :error -> acc
      end
    end)
  end

  defp maybe_refresh_partition_map(state, node) do
    case state.transport.info(node.conn, ["replicas"]) do
      {:ok, %{"replicas" => value}} ->
        apply_replicas(state, node, value)

      {:ok, _other} ->
        register_failure(state, node.name)

      {:error, %Error{} = err} ->
        Logger.debug("Aerospike.Tender: #{node.name} replicas failed: #{err.message}")
        register_failure(state, node.name)
    end
  end

  defp apply_replicas(state, node, value) do
    segments = PartitionMapParser.parse_replicas_with_regime(value)

    Enum.each(segments, fn {namespace, regime, ownership} ->
      apply_ownership(state, node.name, namespace, regime, ownership)
    end)

    update_node(state, node.name, fn n -> %{n | failures: 0} end)
  end

  defp apply_ownership(state, node_name, namespace, regime, ownership) do
    if namespace in state.namespaces do
      Enum.each(ownership, fn {partition_id, replica_index} ->
        merge_replica(state.owners_tab, namespace, partition_id, regime, replica_index, node_name)
      end)
    end
  end

  defp merge_replica(tab, namespace, partition_id, regime, replica_index, node_name) do
    replicas =
      case PartitionMap.owners(tab, namespace, partition_id) do
        {:ok, %{regime: current_regime, replicas: current}} when current_regime == regime ->
          place_replica(current, replica_index, node_name)

        {:ok, %{regime: current_regime}} when current_regime > regime ->
          :stale

        _ ->
          place_replica([], replica_index, node_name)
      end

    case replicas do
      :stale -> :ok
      replicas -> PartitionMap.update(tab, namespace, partition_id, regime, replicas)
    end
  end

  defp place_replica(replicas, replica_index, node_name) do
    padded = pad_replicas(replicas, replica_index)
    List.replace_at(padded, replica_index, node_name)
  end

  defp pad_replicas(replicas, replica_index) do
    needed = replica_index + 1 - length(replicas)

    if needed > 0 do
      replicas ++ List.duplicate(nil, needed)
    else
      replicas
    end
  end

  ## Ready flag

  defp recompute_ready(state) do
    ready? =
      Enum.all?(state.namespaces, fn namespace ->
        PartitionMap.complete?(state.owners_tab, namespace)
      end)

    :ets.insert(state.meta_tab, {:ready, ready?})
    %{state | ready?: ready?}
  end

  ## Pool lifecycle

  # Starts a NodePool for `node_name` under the configured NodeSupervisor.
  # Returns `{:ok, pool_pid}` on success. Returns `:error` on pool-start
  # failure: a warning is logged, the node is skipped for this cycle, and
  # the caller is expected to *not* register the node (per the Phase
  # decision: pool-start failure never bumps the per-node failure counter
  # because the node is never added to `state.nodes`). When the Tender
  # runs without a `:node_supervisor` (cluster-state-only mode), this
  # returns a sentinel `{:ok, nil}` so bootstrap/peer discovery still
  # register the node — that path is used by the invariant tests.
  defp ensure_pool(%{node_supervisor: nil}, _node_name, _host, _port), do: {:ok, nil}

  defp ensure_pool(state, node_name, host, port) do
    opts = [
      node_name: node_name,
      transport: state.transport,
      host: host,
      port: port,
      connect_opts: state.connect_opts,
      pool_size: state.pool_size
    ]

    case NodeSupervisor.start_pool(state.node_supervisor, opts) do
      {:ok, pool_pid} ->
        {:ok, pool_pid}

      {:error, reason} ->
        Logger.warning(
          "Aerospike.Tender: could not start pool for #{node_name} at #{host}:#{port}: " <>
            "#{inspect(reason)}"
        )

        :error
    end
  end

  # Stops the pool for `node`. Absence of a pool (no supervisor or a nil
  # pool_pid) is a no-op. `{:error, :not_found}` from the NodeSupervisor
  # is logged at `:debug` — a pool that already exited is an acceptable
  # terminal state.
  defp drop_pool(%{node_supervisor: nil}, _node), do: :ok
  defp drop_pool(_state, %{pool_pid: nil}), do: :ok

  defp drop_pool(state, %{name: name, pool_pid: pool_pid}) do
    case NodeSupervisor.stop_pool(state.node_supervisor, pool_pid) do
      :ok ->
        :ok

      {:error, :not_found} ->
        Logger.debug("Aerospike.Tender: pool for #{name} already gone")
        :ok
    end
  end

  # On Tender (re)start under `rest_for_one`, the NodeSupervisor survives.
  # Pools started by a previous Tender incarnation are still children of
  # that supervisor but are no longer reachable via the new Tender's
  # `state.nodes`. Kill them before the first tend cycle so the Tender's
  # view remains the single source of truth.
  defp cleanup_orphan_pools(%{node_supervisor: nil}), do: :ok

  defp cleanup_orphan_pools(state) do
    case sup_pid(state.node_supervisor) do
      nil ->
        :ok

      sup ->
        sup
        |> DynamicSupervisor.which_children()
        |> Enum.each(fn
          {_id, pid, _type, _mods} when is_pid(pid) ->
            _ = NodeSupervisor.stop_pool(state.node_supervisor, pid)

          _ ->
            :ok
        end)
    end
  end

  defp sup_pid(name) when is_atom(name), do: Process.whereis(name)
  defp sup_pid(pid) when is_pid(pid), do: pid

  ## Helpers

  defp sorted_nodes(nodes) do
    nodes
    |> Map.values()
    |> Enum.sort_by(& &1.name)
  end

  defp maybe_schedule_tend(%{tend_trigger: :manual} = state), do: state

  defp maybe_schedule_tend(state) do
    Process.send_after(self(), :tend, state.tend_interval_ms)
    state
  end

  defp safe_close(transport, conn) do
    transport.close(conn)
  rescue
    _ -> :ok
  catch
    _, _ -> :ok
  end

  defp read_ready(meta_tab) do
    case :ets.lookup(meta_tab, :ready) do
      [{:ready, ready?}] -> ready?
      [] -> false
    end
  end
end
