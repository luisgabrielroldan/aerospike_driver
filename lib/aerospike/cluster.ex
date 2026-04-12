defmodule Aerospike.Cluster do
  @moduledoc false
  # Manages the lifecycle of the cluster view for one named Aerospike connection.
  #
  # Responsibilities:
  # - Seed discovery: connects to seed hosts, identifies the first reachable node.
  # - Peer discovery: uses `peers-clear-std` info command to find additional nodes.
  # - Partition map: builds and refreshes the partition-to-node routing table in ETS.
  # - Tend loop: periodically re-discovers peers and refreshes partitions.
  #
  # State is stored in two places:
  # - GenServer state: tend connections, generation counters, config.
  # - ETS tables (via Tables): nodes registry, partition map, cluster metadata.

  use GenServer

  require Logger

  alias Aerospike.CircuitBreaker
  alias Aerospike.Connection
  alias Aerospike.NodeSupervisor
  alias Aerospike.Protocol.PartitionMap
  alias Aerospike.Protocol.Peers
  alias Aerospike.Tables

  @default_connect_timeout 5_000
  # How often the tend loop fires, in milliseconds.
  @default_tend_interval 1_000
  @default_pool_size 10

  @typedoc """
  One row in the nodes ETS table (`Tables.nodes/1`): TCP endpoint, pool pid, liveness, features, rack id.
  """
  @type node_row :: %{
          host: String.t(),
          port: non_neg_integer(),
          pool_pid: pid(),
          active: boolean(),
          features: MapSet.t(),
          rack_id: term()
        }

  @doc false
  def child_spec(opts) when is_list(opts) do
    name = Keyword.fetch!(opts, :name)

    %{
      id: {__MODULE__, name},
      start: {__MODULE__, :start_link, [opts]},
      type: :worker,
      restart: :permanent,
      shutdown: 10_000
    }
  end

  @doc false
  def start_link(opts) when is_list(opts) do
    name = Keyword.fetch!(opts, :name)
    GenServer.start_link(__MODULE__, opts, name: cluster_name(name))
  end

  @doc false
  def cluster_name(name) when is_atom(name), do: :"#{name}_cluster"

  @doc false
  @spec rotate_auth_credential(atom(), String.t(), binary()) :: :ok | {:error, :cluster_not_found}
  def rotate_auth_credential(name, user_name, credential)
      when is_atom(name) and is_binary(user_name) and is_binary(credential) do
    case Process.whereis(cluster_name(name)) do
      nil -> {:error, :cluster_not_found}
      pid -> GenServer.call(pid, {:rotate_auth_credential, user_name, credential})
    end
  end

  @impl true
  def init(opts) when is_list(opts) do
    Process.flag(:trap_exit, true)
    name = Keyword.fetch!(opts, :name)
    hosts = Keyword.fetch!(opts, :hosts)
    seeds = Enum.map(hosts, &parse_seed/1)

    # Persist per-command policy defaults so CRUD can merge them at call time.
    policy_defaults = Keyword.get(opts, :defaults, [])
    :ets.insert(Tables.meta(name), {:policy_defaults, policy_defaults})
    :ets.insert(Tables.meta(name), {:auth_opts, Keyword.get(opts, :auth_opts, [])})
    max_error_rate = Keyword.get(opts, :max_error_rate, 100)
    error_rate_window = Keyword.get(opts, :error_rate_window, 1)

    :ets.insert(
      Tables.meta(name),
      {:breaker_config, %{max_error_rate: max_error_rate, error_rate_window: error_rate_window}}
    )

    state = %{
      name: name,
      # Parsed seed addresses as `{host, port}` tuples.
      seeds: seeds,
      connect_timeout: Keyword.get(opts, :connect_timeout, @default_connect_timeout),
      tend_interval: Keyword.get(opts, :tend_interval, @default_tend_interval),
      pool_size: Keyword.get(opts, :pool_size, @default_pool_size),
      auth_opts: Keyword.get(opts, :auth_opts, []),
      recv_timeout: Keyword.get(opts, :recv_timeout, @default_connect_timeout),
      tls: Keyword.get(opts, :tls, false),
      tls_opts: Keyword.get(opts, :tls_opts, []),
      # Server-reported generation counters; nil until first successful tend.
      partition_generation: nil,
      peers_generation: nil,
      # Long-lived info-only connections kept open between tend cycles, keyed by node name.
      tend_conns: %{},
      node_supervisor: nil,
      # Set to true after the first successful initial tend. While false, the
      # tend timer retries seed bootstrap instead of running periodic maintenance.
      bootstrapped: false,
      tend_tick: 0,
      error_rate_window: error_rate_window
    }

    {:ok, state, {:continue, :initial_tend}}
  end

  @impl true
  def terminate(_reason, state) do
    Enum.each(state.tend_conns, fn {_node, conn} -> Connection.close(conn) end)
    :ok
  end

  # Initial tend runs synchronously in handle_continue so the cluster is ready
  # before the first user request. If seeds are unreachable, the GenServer stays
  # alive and retries on the next tend cycle rather than crash-looping through
  # the supervisor.
  @impl true
  def handle_continue(:initial_tend, state) do
    sup = Process.whereis(NodeSupervisor.sup_name(state.name))

    if sup == nil do
      {:stop, {:shutdown, :node_supervisor_unavailable}, state}
    else
      state = %{state | node_supervisor: sup}

      case do_initial_tend(state) do
        {:ok, new_state} ->
          _ = schedule_tend(new_state)
          {:noreply, %{new_state | bootstrapped: true}}

        {:error, reason} ->
          Logger.warning(
            "Cluster #{state.name}: seed connection failed (#{inspect(reason)}), retrying"
          )

          _ = schedule_tend(state)
          {:noreply, state}
      end
    end
  end

  # Not yet bootstrapped — retry seed connection on each tend tick.
  @impl true
  def handle_info(:tend, %{bootstrapped: false} = state) do
    case do_initial_tend(state) do
      {:ok, new_state} ->
        Logger.info("Cluster #{state.name}: seed connected, cluster ready")
        _ = schedule_tend(new_state)
        {:noreply, %{new_state | bootstrapped: true}}

      {:error, _reason} ->
        _ = schedule_tend(state)
        {:noreply, state}
    end
  end

  # Periodic tend is best-effort: failures are swallowed so the cluster stays up.
  @impl true
  def handle_info(:tend, %{bootstrapped: true} = state) do
    state = %{state | tend_tick: state.tend_tick + 1}
    CircuitBreaker.maybe_reset_window(state.name, state.tend_tick, state.error_rate_window)
    new_state = do_periodic_tend(state)
    _ = schedule_tend(new_state)
    {:noreply, new_state}
  end

  @impl true
  def handle_call({:rotate_auth_credential, user_name, credential}, _from, state) do
    auth_opts =
      state.auth_opts
      |> Keyword.put(:user, user_name)
      |> Keyword.put(:credential, credential)

    :ets.insert(Tables.meta(state.name), {:auth_opts, auth_opts})

    state =
      state
      |> Map.put(:auth_opts, auth_opts)
      |> restart_node_pools()

    {:reply, :ok, state}
  end

  # Connects to a seed, discovers peers, builds the partition map, and marks
  # the cluster ready in ETS. This is the full bootstrap sequence.
  defp do_initial_tend(state) do
    with {:ok, conn, node_name, host, port} <- connect_first_seed(state),
         {:ok, pool_pid} <- ensure_node_pool(state, node_name, host, port),
         :ok <- insert_node_registry(state, node_name, host, port, pool_pid),
         {:ok, conn, peers_gen} <- discover_peers(state, conn, node_name),
         {:ok, state} <- build_partition_map(state) do
      state = %{
        state
        | tend_conns: Map.put(state.tend_conns, node_name, conn),
          peers_generation: peers_gen
      }

      mark_cluster_ready(state)
      {:ok, state}
    end
  end

  # Asks the seed node for its peer list via the `peers-clear-std` info command.
  # Non-fatal: returns `{:ok, conn, nil}` if peer discovery is unavailable.
  defp discover_peers(state, conn, seed_node_name) do
    with {:ok, conn, info} <- Connection.request_info(conn, ["peers-clear-std"]),
         raw when is_binary(raw) <- Map.get(info, "peers-clear-std"),
         {:ok, %{generation: gen, peers: peers}} <- Peers.parse_peers_clear_std(raw) do
      add_peer_nodes(state, peers, seed_node_name)
      {:ok, conn, gen}
    else
      _ -> {:ok, conn, nil}
    end
  end

  # Registers each peer that isn't already known and isn't the seed itself.
  defp add_peer_nodes(state, peers, seed_node_name) do
    for %{node_name: node_name, host: host, port: port} <- peers,
        node_name != seed_node_name,
        not node_registered?(state, node_name) do
      add_peer_node(state, node_name, host, port)
    end
  end

  # Connects to a peer, verifies its node name matches what the seed reported,
  # then starts a connection pool and registers the node.
  defp add_peer_node(state, node_name, host, port) do
    conn_opts = connection_opts(state, host, port)

    case Connection.connect(conn_opts) do
      {:ok, conn} ->
        case complete_peer_handshake(conn, state, node_name, host, port) do
          {:ok, conn} ->
            {:ok, conn}

          :error ->
            Connection.close(conn)
            :error
        end

      {:error, _} ->
        :error
    end
  end

  defp complete_peer_handshake(conn, state, node_name, host, port) do
    with {:ok, conn} <- Connection.login(conn, state.auth_opts),
         {:ok, conn, info} <- Connection.request_info(conn, ["node"]),
         {:ok, verified_name} <- extract_node_name(info),
         true <- verified_name == node_name,
         {:ok, pool_pid} <- ensure_node_pool(state, node_name, host, port),
         :ok <- insert_node_registry(state, node_name, host, port, pool_pid) do
      {:ok, conn}
    else
      _ -> :error
    end
  end

  defp node_registered?(state, node_name) do
    :ets.lookup(Tables.nodes(state.name), node_name) != []
  end

  # Rebuilds the full partition map by querying `replicas` from every known node.
  # Clears existing partitions first so stale entries don't linger.
  defp build_partition_map(state) do
    nodes = :ets.tab2list(Tables.nodes(state.name))
    :ets.delete_all_objects(Tables.partitions(state.name))

    {conns, gen} =
      Enum.reduce(nodes, {state.tend_conns, state.partition_generation}, fn
        {node_name, %{host: host, port: port}}, {conns, gen} ->
          case fetch_node_replicas(state, conns, node_name, host, port) do
            {:ok, conn, node_gen} ->
              {Map.put(conns, node_name, conn), node_gen || gen}

            :error ->
              {conns, gen}
          end
      end)

    {:ok, %{state | tend_conns: conns, partition_generation: gen}}
  end

  # Fetches `partition-generation` and `replicas` info from a single node,
  # then inserts that node's partition ownership into ETS.
  defp fetch_node_replicas(state, conns, node_name, host, port) do
    with {:ok, conn} <- get_or_connect(conns, node_name, host, port, state),
         {:ok, conn, info} <- Connection.request_info(conn, ["partition-generation", "replicas"]) do
      insert_node_partitions(state, info, node_name)
      gen = extract_partition_generation(info)
      {:ok, conn, gen}
    else
      _ -> :error
    end
  end

  # Decodes the base64 partition bitmaps and inserts `{namespace, partition_id, replica_index}`
  # tuples into the partitions ETS table, keyed to `node_name`.
  defp insert_node_partitions(state, info, node_name) do
    replicas = Map.get(info, "replicas", "")
    tuples = PartitionMap.parse_replicas_value(replicas, node_name)

    :ets.insert(
      Tables.partitions(state.name),
      Enum.map(tuples, fn {ns, pid, ridx, nn} -> {{ns, pid, ridx}, nn} end)
    )
  end

  defp extract_partition_generation(info) do
    case Map.get(info, "partition-generation") do
      nil ->
        nil

      gen_s ->
        case PartitionMap.parse_partition_generation(gen_s) do
          {:ok, g} -> g
          :error -> nil
        end
    end
  end

  # Reuses an existing tend connection if available; opens a new one otherwise.
  defp get_or_connect(conns, node_name, host, port, state) do
    case Map.get(conns, node_name) do
      nil ->
        conn_opts = connection_opts(state, host, port)

        with {:ok, conn} <- Connection.connect(conn_opts),
             {:ok, conn} <- Connection.login(conn, state.auth_opts) do
          {:ok, conn}
        else
          _ -> :error
        end

      conn ->
        {:ok, conn}
    end
  end

  # Periodic tend: refresh peers first (may discover new nodes), then partitions.
  defp do_periodic_tend(state) do
    state
    |> tend_refresh_peers()
    |> tend_refresh_partitions()
  end

  # Asks one tend connection for the current peer list; skips if generation hasn't changed.
  defp tend_refresh_peers(state) do
    case Map.to_list(state.tend_conns) do
      [] ->
        state

      [{node_name, conn} | _] ->
        do_tend_refresh_peers(state, node_name, conn)
    end
  end

  defp do_tend_refresh_peers(state, node_name, conn) do
    with {:ok, conn, info} <- Connection.request_info(conn, ["peers-clear-std"]),
         raw when is_binary(raw) <- Map.get(info, "peers-clear-std"),
         {:ok, %{generation: gen, peers: peers}} <- Peers.parse_peers_clear_std(raw),
         true <- gen != state.peers_generation do
      add_peer_nodes(state, peers, node_name)

      state
      |> prune_departed_peers(peers, node_name)
      |> Map.put(:peers_generation, gen)
      |> Map.update!(:tend_conns, &Map.put(&1, node_name, conn))
    else
      _ -> state
    end
  end

  defp prune_departed_peers(state, peers, reporting_node_name) do
    current_peer_names = MapSet.new(Enum.map(peers, & &1.node_name))

    Tables.nodes(state.name)
    |> :ets.tab2list()
    |> Enum.map(fn {node_name, _row} -> node_name end)
    |> Enum.reduce(state, fn
      ^reporting_node_name, st ->
        st

      node_name, st ->
        if MapSet.member?(current_peer_names, node_name) do
          st
        else
          prune_departed_peer(st, node_name)
        end
    end)
  end

  defp prune_departed_peer(state, node_name) do
    case :ets.lookup(Tables.nodes(state.name), node_name) do
      [{^node_name, %{pool_pid: pool_pid}}] when is_pid(pool_pid) ->
        _ = stop_pool_and_wait(state.node_supervisor, pool_pid)

      _ ->
        :ok
    end

    :ets.delete(Tables.nodes(state.name), node_name)
    _ = :ets.select_delete(Tables.partitions(state.name), [{{:"$1", node_name}, [], [true]}])

    case Map.pop(state.tend_conns, node_name) do
      {nil, tend_conns} ->
        %{state | tend_conns: tend_conns}

      {conn, tend_conns} ->
        _ = Connection.close(conn)
        %{state | tend_conns: tend_conns}
    end
  end

  # Checks each node's partition-generation; only re-fetches replicas when the
  # generation has advanced (meaning the cluster rebalanced).
  defp tend_refresh_partitions(state) do
    Enum.reduce(state.tend_conns, state, fn {node_name, conn}, st ->
      case refresh_node(st, node_name, conn) do
        {:ok, conn2, new_gen} ->
          %{
            st
            | tend_conns: Map.put(st.tend_conns, node_name, conn2),
              partition_generation: new_gen
          }

        {:error, _} ->
          st
      end
    end)
  end

  # Probes a single node for its partition-generation; triggers a re-fetch if changed.
  defp refresh_node(state, node_name, conn) do
    case Connection.request_info(conn, ["node", "partition-generation"]) do
      {:ok, conn2, map} ->
        check_partition_generation(state, conn2, node_name, Map.get(map, "partition-generation"))

      {:error, _} = err ->
        err
    end
  end

  defp check_partition_generation(state, conn, _node_name, nil) do
    {:ok, conn, state.partition_generation}
  end

  defp check_partition_generation(state, conn, node_name, gen_string) do
    case PartitionMap.parse_partition_generation(gen_string) do
      {:ok, gen} when gen != state.partition_generation ->
        refetch_partition_map_for_node(state, conn, node_name)

      _ ->
        {:ok, conn, state.partition_generation}
    end
  end

  # Re-queries `replicas` for one node and merges the result into the partitions table.
  # Unlike `build_partition_map/1`, this does not clear stale entries — it only overwrites
  # partitions owned by this specific node.
  defp refetch_partition_map_for_node(state, conn, node_name) do
    case Connection.request_info(conn, ["partition-generation", "replicas"]) do
      {:ok, conn, info} ->
        replicas = Map.get(info, "replicas", "")
        gen_s = Map.get(info, "partition-generation")

        gen =
          case gen_s && PartitionMap.parse_partition_generation(gen_s) do
            {:ok, g} -> g
            _ -> state.partition_generation
          end

        tuples = PartitionMap.parse_replicas_value(replicas, node_name)

        Enum.each(tuples, fn {ns, pid, ridx, nn} ->
          :ets.insert(Tables.partitions(state.name), {{ns, pid, ridx}, nn})
        end)

        {:ok, conn, gen}

      {:error, _} = err ->
        err
    end
  end

  # Tries each seed address in order; returns the first successful connection.
  defp connect_first_seed(state) do
    Enum.reduce_while(state.seeds, {:error, :no_seeds}, fn {host, port}, _acc ->
      case try_seed(state, host, port) do
        {:ok, _, _, _, _} = ok -> {:halt, ok}
        {:error, reason} -> {:cont, {:error, reason}}
      end
    end)
  end

  # Opens a TCP connection to a seed and performs the login + info handshake.
  # Closes the socket on handshake failure to avoid leaking file descriptors.
  defp try_seed(state, host, port) do
    conn_opts = connection_opts(state, host, port)

    with {:ok, conn} <- Connection.connect(conn_opts) do
      case seed_handshake(conn, state) do
        {:ok, conn, node_name} ->
          {:ok, conn, node_name, host, port}

        {:error, _} = err ->
          _ = Connection.close(conn)
          err
      end
    end
  end

  # Authenticates and queries `node` + `build` info to identify the seed.
  defp seed_handshake(conn, state) do
    with {:ok, conn} <- Connection.login(conn, state.auth_opts),
         {:ok, conn, map} <- Connection.request_info(conn, ["node", "build"]),
         {:ok, node_name} <- extract_node_name(map) do
      {:ok, conn, node_name}
    end
  end

  defp extract_node_name(%{"node" => node_name}) when is_binary(node_name), do: {:ok, node_name}
  defp extract_node_name(_), do: {:error, :no_node_field}

  # Starts a NimblePool for the node, or returns the existing one if already running.
  defp ensure_node_pool(state, node_name, host, port) do
    opts = [
      node_name: node_name,
      pool_size: state.pool_size,
      connect_opts: connection_opts(state, host, port),
      auth_opts: state.auth_opts
    ]

    case NodeSupervisor.start_pool(state.node_supervisor, opts) do
      {:ok, pool_pid} -> {:ok, pool_pid}
      {:error, {:already_started, pool_pid}} -> {:ok, pool_pid}
      {:error, :already_present} -> {:error, :pool_already_present}
      {:error, _} = err -> err
    end
  end

  defp restart_node_pools(state) do
    Enum.reduce(:ets.tab2list(Tables.nodes(state.name)), state, fn {node_name, row}, acc ->
      restart_node_pool(acc, node_name, row)
    end)
  end

  defp restart_node_pool(state, node_name, %{host: host, port: port, pool_pid: pool_pid} = row) do
    _ = stop_pool_and_wait(state.node_supervisor, pool_pid)

    case ensure_node_pool_with_retry(state, node_name, host, port, pool_pid) do
      {:ok, new_pool_pid} ->
        :ets.insert(Tables.nodes(state.name), {node_name, %{row | pool_pid: new_pool_pid}})
        state

      {:error, _reason} ->
        state
    end
  end

  defp ensure_node_pool_with_retry(
         state,
         node_name,
         host,
         port,
         previous_pool_pid,
         attempts \\ 100
       )
       when is_binary(node_name) and is_binary(host) and is_integer(port) and is_integer(attempts) and
              attempts >= 0 and is_pid(previous_pool_pid) do
    case ensure_node_pool(state, node_name, host, port) do
      {:ok, pool_pid} when is_pid(pool_pid) ->
        handle_restarted_pool_pid(
          state,
          node_name,
          host,
          port,
          previous_pool_pid,
          attempts,
          pool_pid
        )

      {:error, _reason} ->
        retry_ensure_node_pool(state, node_name, host, port, previous_pool_pid, attempts)
    end
  end

  defp handle_restarted_pool_pid(
         state,
         node_name,
         host,
         port,
         previous_pool_pid,
         attempts,
         pool_pid
       )
       when is_pid(previous_pool_pid) and is_integer(attempts) and is_pid(pool_pid) do
    if valid_restarted_pool_pid?(pool_pid, previous_pool_pid) do
      {:ok, pool_pid}
    else
      retry_ensure_node_pool(state, node_name, host, port, previous_pool_pid, attempts)
    end
  end

  defp valid_restarted_pool_pid?(pool_pid, previous_pool_pid)
       when is_pid(pool_pid) and is_pid(previous_pool_pid) do
    Process.alive?(pool_pid) and pool_pid != previous_pool_pid
  end

  defp retry_ensure_node_pool(state, node_name, host, port, previous_pool_pid, attempts)
       when is_integer(attempts) and attempts > 0 do
    Process.sleep(50)
    ensure_node_pool_with_retry(state, node_name, host, port, previous_pool_pid, attempts - 1)
  end

  defp retry_ensure_node_pool(_state, _node_name, _host, _port, _previous_pool_pid, 0) do
    {:error, :pool_restart_timeout}
  end

  defp stop_pool_and_wait(node_supervisor, pool_pid, timeout \\ 1_000)
       when is_pid(pool_pid) and is_integer(timeout) and timeout >= 0 do
    monitor = if Process.alive?(pool_pid), do: Process.monitor(pool_pid), else: nil
    result = NodeSupervisor.stop_pool(node_supervisor, pool_pid)

    case monitor do
      nil ->
        result

      ref ->
        receive do
          {:DOWN, ^ref, :process, ^pool_pid, _reason} ->
            result
        after
          timeout ->
            Process.demonitor(ref, [:flush])
            result
        end
    end
  end

  # Writes the node's metadata into the nodes ETS table. The Router reads
  # this to look up pool PIDs when routing requests.
  @spec insert_node_registry(map(), String.t(), String.t(), non_neg_integer(), pid()) :: :ok
  defp insert_node_registry(state, node_name, host, port, pool_pid) do
    row = node_registry_row(host, port, pool_pid)
    :ets.insert(Tables.nodes(state.name), {node_name, row})
    :ok
  end

  @spec node_registry_row(String.t(), non_neg_integer(), pid()) :: node_row()
  defp node_registry_row(host, port, pool_pid) do
    %{
      host: host,
      port: port,
      # PID of the NimblePool for this node.
      pool_pid: pool_pid,
      active: true,
      # Server feature flags; populated in later phases.
      features: MapSet.new(),
      # Rack-aware routing; nil until rack config is supported.
      rack_id: nil
    }
  end

  # Signals to the Router that the cluster is ready to serve requests.
  defp mark_cluster_ready(state) do
    :ets.insert(Tables.meta(state.name), {Tables.ready_key(), true})
    :ok
  end

  defp schedule_tend(state) do
    Process.send_after(self(), :tend, state.tend_interval)
  end

  defp connection_opts(state, host, port) do
    [
      host: host,
      port: port,
      timeout: state.connect_timeout,
      recv_timeout: state.recv_timeout,
      tls: state.tls,
      tls_opts: state.tls_opts
    ]
  end

  # Parses "host:port" seed strings; defaults to port 3000 when omitted.
  defp parse_seed(host_port) when is_binary(host_port) do
    case String.split(host_port, ":", parts: 2) do
      [h, p] -> {h, String.to_integer(p)}
      [h] -> {h, 3000}
    end
  end
end
