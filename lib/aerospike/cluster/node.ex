defmodule Aerospike.Cluster.Node do
  @moduledoc false

  alias Aerospike.Error
  alias Aerospike.Protocol.Info, as: InfoParser
  alias Aerospike.Protocol.PartitionMap, as: PartitionMapParser
  alias Aerospike.Protocol.Peers

  require Logger

  @refresh_info_keys ["partition-generation", "cluster-stable", "peers-generation"]

  @enforce_keys [:name, :host, :port, :conn]
  defstruct [
    :name,
    :host,
    :port,
    :conn,
    session: nil,
    features: nil,
    generation_seen: nil,
    applied_gen: nil,
    cluster_stable: nil,
    peers_generation_seen: nil
  ]

  @typedoc """
  Cached login session returned by `Aerospike.Cluster.NodeTransport.login/2`:

    * `nil` — no credentials configured, or the server has security
      disabled / accepted the login without issuing a token.
    * `{token, expires_at_monotonic_ms_or_nil}` — the server issued
      `token` and its TTL has been translated into a monotonic-time
      deadline (minus a small skew) so the caller can compare against
      `System.monotonic_time(:millisecond)` directly.
  """
  @type session :: nil | {token :: binary(), expires_at_ms :: integer() | nil}

  @type t :: %__MODULE__{
          name: String.t(),
          host: String.t(),
          port: :inet.port_number(),
          conn: term(),
          session: session(),
          features: MapSet.t() | nil,
          generation_seen: non_neg_integer() | nil,
          applied_gen: non_neg_integer() | nil,
          cluster_stable: String.t() | nil,
          peers_generation_seen: non_neg_integer() | nil
        }

  @typedoc """
  Options used by `seed/5` and `login/4` to drive the admin-protocol
  login on a freshly opened info socket.

    * `:user` / `:password` — both must be present for login to run. When
      either is `nil`, `login/4` returns `{:ok, nil}` and no auth traffic
      is sent. Matches the Tender's `validate_auth_pair!/2` contract.
  """
  @type auth_opts :: [user: String.t() | nil, password: String.t() | nil]

  @typedoc """
  Observations produced by `refresh/2` in addition to the updated struct:

    * `:partition_generation` — the parsed `partition-generation` value
      the node reported this refresh. The caller compares against its own
      per-node-generation store to decide whether to refetch `replicas`.
    * `:cluster_stable` — the raw `cluster-stable` hash reported this
      refresh. The tend cycle folds these hashes into a per-cycle map and
      feeds them to `PartitionMapMerge.verify_cluster_stable/1` (extracted
      in a later step of the split).
    * `:peers_generation` — the parsed `peers-generation` value the node
      reported this refresh.
    * `:peers_generation_changed?` — `true` when the freshly observed
      `peers-generation` differs from the cached `peers_generation_seen`
      on the incoming struct. A freshly-seen node (cache is `nil`) always
      reports `true`, matching the Go/C/Java clients' first-cycle sentinel
      behaviour. The tend cycle uses this flag to decide whether any node
      warrants a `peers-clear-std` probe this cycle.
  """
  @type refresh_observations :: %{
          partition_generation: non_neg_integer(),
          cluster_stable: String.t(),
          peers_generation: non_neg_integer(),
          peers_generation_changed?: boolean()
        }

  @doc """
  Dials `host:port` through `transport`, runs the admin-protocol login (if
  credentials are supplied), and fetches the `node` and `features` info
  keys in one round trip.

  Returns `{:ok, %Node{}}` with the node name reported by the server, the
  parsed feature set, and the cached session. Returns `{:error, reason}`
  on any step's failure; the caller is expected to close/ignore the
  connection (a bootstrap failure surfaces before `conn` is owned by any
  struct).

  `connect_opts` is forwarded verbatim to `transport.connect/3`.
  """
  @spec seed(module(), String.t(), :inet.port_number(), keyword(), auth_opts()) ::
          {:ok, t()} | {:error, :no_node_info | Error.t()}
  def seed(transport, host, port, connect_opts, auth_opts) do
    with {:ok, conn} <- transport.connect(host, port, connect_opts),
         {:ok, session} <- login(transport, conn, auth_opts),
         {:ok, %{"node" => node_name} = info} <- transport.info(conn, ["node", "features"]) do
      features = parse_features(node_name, info)

      node = %__MODULE__{
        name: node_name,
        host: host,
        port: port,
        conn: conn,
        session: session,
        features: features,
        generation_seen: nil,
        applied_gen: nil,
        cluster_stable: nil,
        peers_generation_seen: nil
      }

      {:ok, node}
    else
      {:error, %Error{}} = err ->
        err

      {:ok, _other} ->
        {:error, :no_node_info}
    end
  end

  @doc """
  Runs the admin-protocol login on an already-connected info socket.

  Returns `{:ok, nil}` when no credentials are configured, when the server
  responded with `:ok_no_token` (PKI-anonymous), or when security is
  disabled. Returns `{:ok, {token, expires_at_monotonic_ms | nil}}` on a
  successful session-token login. Any transport-level error propagates as
  `{:error, %Error{}}`.

  `expires_at_monotonic_ms` is `System.monotonic_time(:millisecond)` plus
  the server-reported TTL (seconds), minus a small skew so the client
  drops the token before the server does. Matches Go's
  `login_command.go`.
  """
  @spec login(module(), term(), auth_opts()) :: {:ok, session()} | {:error, Error.t()}
  def login(transport, conn, opts) do
    user = Keyword.get(opts, :user)
    password = Keyword.get(opts, :password)

    if is_nil(user) or is_nil(password) do
      {:ok, nil}
    else
      do_login(transport, conn, user, password)
    end
  end

  defp do_login(transport, conn, user, password) do
    case transport.login(conn, user: user, password: password) do
      {:ok, {:session, token, ttl}} ->
        {:ok, {token, session_expires_at(ttl)}}

      {:ok, :ok_no_token} ->
        {:ok, nil}

      {:ok, :security_not_enabled} ->
        {:ok, nil}

      {:error, %Error{}} = err ->
        err
    end
  end

  # `ttl` is in seconds; subtract up to 60 s (or half the TTL, whichever is
  # smaller) so the client drops the token before the server does
  # (matches Go `login_command.go`).
  defp session_expires_at(nil), do: nil

  defp session_expires_at(ttl_seconds) when is_integer(ttl_seconds) and ttl_seconds > 0 do
    skew = min(60, div(ttl_seconds, 2))
    System.monotonic_time(:millisecond) + (ttl_seconds - skew) * 1_000
  end

  defp session_expires_at(_), do: nil

  @doc """
  Parses the `features` info-key reply into a MapSet of capability tokens.

  Missing, non-binary, or otherwise unusable replies collapse to an empty
  MapSet — the safe default (every optional capability stays off) that the
  Tender's bootstrap path has historically taken on probe failure.
  """
  @spec parse_features(String.t(), %{String.t() => String.t()}) :: MapSet.t()
  def parse_features(node_name, info) do
    case Map.fetch(info, "features") do
      {:ok, value} when is_binary(value) ->
        InfoParser.parse_features(value)

      _ ->
        Logger.debug(fn ->
          "Aerospike.Cluster.Node: #{node_name} features key absent; assuming none"
        end)

        MapSet.new()
    end
  end

  @doc """
  Runs one refresh info round trip (`partition-generation`,
  `cluster-stable`, and `peers-generation`) against the node.

  On success returns `{:ok, node, observations}` where `node` carries the
  updated `generation_seen`, `cluster_stable`, and `peers_generation_seen`
  fields, and `observations` exposes the raw values (plus a
  `peers_generation_changed?` flag) so the caller can decide whether a
  subsequent `refresh_partitions/2` or cluster-wide peer refresh is
  warranted this cycle.

  Returns `{:error, reason}` for any transport failure or malformed reply.
  The struct is left untouched on error — the caller is expected to count
  the failure against its own lifecycle budget.
  """
  @spec refresh(t(), module()) ::
          {:ok, t(), refresh_observations()} | {:error, Error.t() | :malformed_reply}
  def refresh(%__MODULE__{} = node, transport) do
    case transport.info(node.conn, @refresh_info_keys) do
      {:ok, info} ->
        handle_refresh_info(node, info)

      {:error, %Error{}} = err ->
        err
    end
  end

  # All three keys must parse for the refresh to count as a success.
  # Missing or malformed `partition-generation` is the canonical failure
  # signal (the rest of the tend cycle hangs off the generation value);
  # missing or malformed `cluster-stable` is also a failure because the
  # agreement guard cannot verify a node that did not report a hash;
  # `peers-generation` is the trigger for cluster-wide peer discovery,
  # and treating its absence as success would silently disable the
  # trigger.
  defp handle_refresh_info(node, %{
         "partition-generation" => gen_value,
         "cluster-stable" => cluster_stable,
         "peers-generation" => peers_gen_value
       })
       when is_binary(cluster_stable) and cluster_stable != "" do
    with {:ok, gen} <- PartitionMapParser.parse_partition_generation(gen_value),
         {:ok, peers_gen} <- Peers.parse_generation(peers_gen_value) do
      changed? = node.peers_generation_seen != peers_gen

      updated = %__MODULE__{
        node
        | generation_seen: gen,
          cluster_stable: cluster_stable,
          peers_generation_seen: peers_gen
      }

      {:ok, updated,
       %{
         partition_generation: gen,
         cluster_stable: cluster_stable,
         peers_generation: peers_gen,
         peers_generation_changed?: changed?
       }}
    else
      :error -> {:error, :malformed_reply}
    end
  end

  defp handle_refresh_info(_node, _info), do: {:error, :malformed_reply}

  @doc """
  Picks the peer-discovery info key for this node based on the cluster's
  `:use_services_alternate` toggle. `peers-clear-alt` surfaces the
  server's alternate-access addresses; `peers-clear-std` surfaces its
  primary-access addresses. Reply format is identical.
  """
  @spec peer_info_key(boolean()) :: String.t()
  def peer_info_key(true), do: "peers-clear-alt"
  def peer_info_key(_), do: "peers-clear-std"

  @doc """
  Fetches and parses the node's peer list.

  The `:use_services_alternate` option chooses between `peers-clear-std`
  (default) and `peers-clear-alt`. Returns `{:ok, node, peers}` where
  `peers` is the list of peer maps from `Aerospike.Protocol.Peers`
  (cross-node accumulation belongs to the caller). Returns
  `{:error, reason}` when the info call fails or the reply cannot be
  parsed; the node struct is unchanged on error.
  """
  @spec refresh_peers(t(), module(), use_services_alternate: boolean()) ::
          {:ok, t(), [map()]} | {:error, Error.t() | :malformed_reply}
  def refresh_peers(%__MODULE__{} = node, transport, opts \\ []) do
    alternate? = Keyword.get(opts, :use_services_alternate, false)
    key = peer_info_key(alternate?)

    case transport.info(node.conn, [key]) do
      {:ok, %{^key => value}} ->
        case Peers.parse_peers_clear_std(value) do
          {:ok, %{peers: peers}} -> {:ok, node, peers}
          :error -> {:error, :malformed_reply}
        end

      {:ok, _other} ->
        {:error, :malformed_reply}

      {:error, %Error{}} = err ->
        err
    end
  end

  @doc """
  Fetches the node's `features` info-key reply on an already-connected
  socket. Used by peer discovery, where `seed/5` is not called (the peer's
  name is already known from the parent node's `peers-clear-std` reply).
  Returns a MapSet of capability tokens; probe failures collapse to the
  empty set.
  """
  @spec fetch_features(module(), term(), String.t()) :: MapSet.t()
  def fetch_features(transport, conn, node_name) do
    case transport.info(conn, ["features"]) do
      {:ok, info} ->
        parse_features(node_name, info)

      {:error, %Error{} = err} ->
        Logger.debug(fn ->
          "Aerospike.Cluster.Node: peer #{node_name} features probe failed: #{err.message}"
        end)

        MapSet.new()
    end
  end

  @doc """
  Fetches and parses the node's `replicas` reply.

  Returns `{:ok, node, segments}` where `segments` is the list
  `[{namespace, regime, ownership}]` produced by
  `Aerospike.Protocol.PartitionMap.parse_replicas_with_regime/1`. The
  caller is responsible for applying those segments against the partition
  map (stale-regime detection, ETS writes); this function only does I/O
  and parsing. Returns `{:error, reason}` on any failure; the struct is
  unchanged.
  """
  @spec refresh_partitions(t(), module()) ::
          {:ok, t(), [{String.t(), non_neg_integer(), [{non_neg_integer(), non_neg_integer()}]}]}
          | {:error, Error.t() | :malformed_reply}
  def refresh_partitions(%__MODULE__{} = node, transport) do
    case transport.info(node.conn, ["replicas"]) do
      {:ok, %{"replicas" => value}} ->
        segments = PartitionMapParser.parse_replicas_with_regime(value)
        {:ok, node, segments}

      {:ok, _other} ->
        {:error, :malformed_reply}

      {:error, %Error{}} = err ->
        err
    end
  end

  @doc """
  Clears the per-cycle `cluster_stable` hash on the struct. Called at the
  start of every tend cycle so `verify_cluster_stable/1` only ever
  considers hashes captured within the current cycle.
  """
  @spec clear_cluster_stable(t()) :: t()
  def clear_cluster_stable(%__MODULE__{} = node), do: %__MODULE__{node | cluster_stable: nil}

  @doc """
  Records the fact that the caller successfully applied the node's
  partition-map segments at `gen`. The struct's `applied_gen` moves
  forward; nothing else changes. `gen` may be `nil` when a freshly
  discovered peer reaches this stage before its first successful
  `partition-generation` probe — matches the pre-split behaviour where
  `applied_gen: nil` naturally re-triggers a fetch on the next cycle.
  """
  @spec mark_partition_map_applied(t(), non_neg_integer() | nil) :: t()
  def mark_partition_map_applied(%__MODULE__{} = node, gen)
      when is_nil(gen) or is_integer(gen) do
    %__MODULE__{node | applied_gen: gen}
  end

  @doc """
  Closes the node's info socket through `transport`. Swallows any exit or
  exception so `terminate/2` and lifecycle transitions can call it
  unconditionally.
  """
  @spec close(module(), term()) :: :ok
  def close(transport, conn) do
    transport.close(conn)
  rescue
    _ -> :ok
  catch
    _, _ -> :ok
  end
end
