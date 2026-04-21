defmodule Aerospike.Supervisor do
  @moduledoc """
  Top-level supervisor for one named Aerospike cluster.

  Starts three children under `rest_for_one` so the blast radius of a
  crash matches which process owns what state:

    1. `Aerospike.TableOwner` — creates and owns the ETS tables backing
       the cluster. Must start first so every later child can read the
       table names.
    2. `Aerospike.NodeSupervisor` — `DynamicSupervisor` for per-node
       `NimblePool` children. Lives independently of the Tender so the
       Tender can restart without losing already-started pools; the
       Tender's restart path sweeps orphans to reconcile.
    3. `Aerospike.Tender` — the single-writer cluster-state GenServer.
       Started last so its `init/1` can read the TableOwner's tables
       and reference the NodeSupervisor by name.

  Crash semantics under `rest_for_one`:

    * Tender crash → only the Tender restarts. ETS tables survive, the
      NodeSupervisor survives, and the restarted Tender rehydrates its
      `:ready` flag from the `:meta` table.
    * NodeSupervisor crash → Tender also restarts (it is after
      NodeSupervisor in the list). TableOwner survives.
    * TableOwner crash → the whole subtree restarts with fresh tables.

  This module only supervises. It does not start pools, does not run
  tend cycles, and does not own any application state.
  """

  alias Aerospike.NodeSupervisor
  alias Aerospike.TableOwner
  alias Aerospike.Tender

  @typedoc """
  Start options.

    * `:name` — atom used as the cluster identity (required). Becomes
      the Tender's registered name, the TableOwner's table prefix, and
      the NodeSupervisor's registered name.
    * `:transport` — module implementing `Aerospike.NodeTransport`
      (required).
    * `:seeds` — list of `{host, port}` tuples (required, non-empty).
    * `:namespaces` — list of namespace strings the cluster must serve
      before `Tender.ready?/1` returns `true` (required, non-empty).

  Every other option is forwarded verbatim to `Aerospike.Tender` (for
  example `:connect_opts`, `:failure_threshold`, `:tend_interval_ms`,
  `:tend_trigger`, `:use_compression`, `:use_services_alternate`,
  `:pool_size`, `:idle_timeout_ms`, `:max_idle_pings`).

  ## Auth opts

  `:user` and `:password` are cluster-wide session-login credentials.
  Both must be present together; neither present disables auth (the
  transport connects plaintext without a login handshake). The
  credentials are forwarded into `:connect_opts` so the transport can
  run the admin-protocol login immediately after the TCP handshake;
  the Tender additionally caches the resulting session token per node
  and reuses it across pool workers.

  Pool-level knobs live at the top level of the keyword list because
  the pool supervisor — not the transport — honours them:

    * `:idle_timeout_ms` — milliseconds a pooled worker may sit idle
      before `NimblePool.handle_ping/2` evicts it. Must be a positive
      integer when set.
    * `:max_idle_pings` — positive integer bounding how many idle
      workers NimblePool may drop per verification cycle.

  TCP-level tuning knobs live inside `:connect_opts` because
  `Aerospike.Transport.Tcp.connect/3` is where they take effect. See
  its moduledoc for the public keys and how they map to
  `:inet.setopts/2` spellings.
  """
  @type option ::
          {:name, atom()}
          | {:transport, module()}
          | {:seeds, [Tender.seed(), ...]}
          | {:namespaces, [Tender.namespace(), ...]}
          | {atom(), term()}

  @doc false
  def child_spec(opts) when is_list(opts) do
    name = Keyword.fetch!(opts, :name)

    %{
      id: {__MODULE__, name},
      start: {__MODULE__, :start_link, [opts]},
      type: :supervisor,
      restart: :permanent,
      shutdown: :infinity
    }
  end

  @doc """
  Starts the top-level supervisor for one named cluster.

  Returns the `Supervisor` pid on success. The supervisor registers
  itself under `sup_name/1` so callers can reach it by name.
  """
  @spec start_link([option()]) :: Supervisor.on_start()
  def start_link(opts) when is_list(opts) do
    validated = validate!(opts)
    name = Keyword.fetch!(validated, :name)

    tender_opts =
      validated
      |> Keyword.drop([:tables, :node_supervisor])
      |> Keyword.put(:node_supervisor, NodeSupervisor.sup_name(name))

    children = [
      {TableOwner, name: name},
      {NodeSupervisor, name: name},
      %{
        id: {Tender, name},
        start: {__MODULE__, :start_tender, [name, tender_opts]},
        type: :worker,
        restart: :permanent,
        shutdown: 5_000
      }
    ]

    Supervisor.start_link(children, strategy: :rest_for_one, name: sup_name(name))
  end

  @doc false
  # Starts the Tender after resolving the TableOwner's tables at supervisor
  # init time. TableOwner is already up at this point (first child under
  # `rest_for_one`), so `tables/1` is a synchronous call on a live process.
  @spec start_tender(atom(), keyword()) :: GenServer.on_start()
  def start_tender(name, tender_opts) do
    tables = TableOwner.tables(TableOwner.via(name))

    tender_opts
    |> Keyword.put(:tables, tables)
    |> Tender.start_link()
  end

  @doc """
  Returns the registered name atom used by `start_link/1` for `name`.
  """
  @spec sup_name(atom()) :: atom()
  def sup_name(name) when is_atom(name), do: :"#{name}_sup"

  ## Validation

  @required_keys [:name, :transport, :seeds, :namespaces]

  defp validate!(opts) do
    Enum.each(@required_keys, fn key ->
      Keyword.has_key?(opts, key) or
        raise ArgumentError, "Aerospike.Supervisor: missing required option #{inspect(key)}"
    end)

    name = Keyword.fetch!(opts, :name)

    is_atom(name) or
      raise ArgumentError, "Aerospike.Supervisor: :name must be an atom, got #{inspect(name)}"

    transport = Keyword.fetch!(opts, :transport)

    is_atom(transport) or
      raise ArgumentError,
            "Aerospike.Supervisor: :transport must be a module, got #{inspect(transport)}"

    seeds = Keyword.fetch!(opts, :seeds)

    (is_list(seeds) and seeds != []) or
      raise ArgumentError,
            "Aerospike.Supervisor: :seeds must be a non-empty list of {host, port} tuples"

    validate_seeds!(seeds)

    namespaces = Keyword.fetch!(opts, :namespaces)

    (is_list(namespaces) and namespaces != []) or
      raise ArgumentError,
            "Aerospike.Supervisor: :namespaces must be a non-empty list of strings"

    validate_namespaces!(namespaces)

    validate_pos_integer!(opts, :pool_size)
    validate_pos_integer!(opts, :idle_timeout_ms)
    validate_pos_integer!(opts, :max_idle_pings)
    validate_pos_integer!(opts, :tend_interval_ms)
    validate_non_neg_integer!(opts, :failure_threshold)
    validate_non_neg_integer!(opts, :circuit_open_threshold)
    validate_pos_integer!(opts, :max_concurrent_ops_per_node)
    validate_non_neg_integer!(opts, :max_retries)
    validate_non_neg_integer!(opts, :sleep_between_retries_ms)

    validate_auth_opts!(opts)
    validate_top_level_bool!(opts, :use_compression)
    validate_top_level_bool!(opts, :use_services_alternate)
    validate_tend_trigger!(opts)
    validate_replica_policy!(opts)
    validate_connect_opts!(opts)

    opts
  end

  # `:user` / `:password` are cluster-wide credentials. Reject a partial
  # pair at `start_link/1` rather than letting a half-configured cluster
  # silently disable auth on one socket and require it on the next.
  defp validate_auth_opts!(opts) do
    user = Keyword.get(opts, :user)
    password = Keyword.get(opts, :password)

    case {user, password} do
      {nil, nil} ->
        :ok

      {u, p} when is_binary(u) and is_binary(p) ->
        :ok

      _ ->
        raise ArgumentError,
              "Aerospike.Supervisor: :user and :password must both be strings or both be absent"
    end
  end

  defp validate_pos_integer!(opts, key) do
    case Keyword.fetch(opts, key) do
      :error -> :ok
      {:ok, value} when is_integer(value) and value > 0 -> :ok
      {:ok, value} -> raise_pos_integer!(key, value)
    end
  end

  defp validate_non_neg_integer!(opts, key) do
    case Keyword.fetch(opts, key) do
      :error ->
        :ok

      {:ok, value} when is_integer(value) and value >= 0 ->
        :ok

      {:ok, value} ->
        raise ArgumentError,
              "Aerospike.Supervisor: #{inspect(key)} must be a non-negative integer, " <>
                "got #{inspect(value)}"
    end
  end

  defp raise_pos_integer!(key, value) do
    raise ArgumentError,
          "Aerospike.Supervisor: #{inspect(key)} must be a positive integer, " <>
            "got #{inspect(value)}"
  end

  defp validate_seeds!(seeds) do
    Enum.each(seeds, &validate_seed!/1)
  end

  defp validate_seed!({host, port}) when is_binary(host) and host != "" and is_integer(port) do
    (port >= 1 and port <= 65_535) or
      raise ArgumentError,
            "Aerospike.Supervisor: seed port must be in 1..65535, got #{inspect(port)}"
  end

  defp validate_seed!(seed) do
    raise ArgumentError,
          "Aerospike.Supervisor: each seed must be a {host, port} tuple with a non-empty " <>
            "string host and integer port, got #{inspect(seed)}"
  end

  defp validate_namespaces!(namespaces) do
    Enum.each(namespaces, fn
      namespace when is_binary(namespace) and namespace != "" ->
        :ok

      namespace ->
        raise ArgumentError,
              "Aerospike.Supervisor: each namespace must be a non-empty string, " <>
                "got #{inspect(namespace)}"
    end)
  end

  defp validate_top_level_bool!(opts, key) do
    case Keyword.fetch(opts, key) do
      :error ->
        :ok

      {:ok, value} when is_boolean(value) ->
        :ok

      {:ok, value} ->
        raise ArgumentError,
              "Aerospike.Supervisor: #{inspect(key)} must be a boolean, got #{inspect(value)}"
    end
  end

  defp validate_tend_trigger!(opts) do
    case Keyword.fetch(opts, :tend_trigger) do
      :error ->
        :ok

      {:ok, value} when value in [:timer, :manual] ->
        :ok

      {:ok, value} ->
        raise ArgumentError,
              "Aerospike.Supervisor: :tend_trigger must be :timer or :manual, " <>
                "got #{inspect(value)}"
    end
  end

  defp validate_replica_policy!(opts) do
    case Keyword.fetch(opts, :replica_policy) do
      :error ->
        :ok

      {:ok, value} when value in [:master, :sequence] ->
        :ok

      {:ok, value} ->
        raise ArgumentError,
              "Aerospike.Supervisor: :replica_policy must be :master or :sequence, " <>
                "got #{inspect(value)}"
    end
  end

  # Rejects the two fat-fingers an operator is most likely to hit inside
  # `:connect_opts`: a non-boolean boolean or a non-positive buffer size.
  # `Transport.Tcp.connect/3` also raises on bad values — doing it here
  # too means `start_link/1` fails synchronously instead of later when
  # the first pool worker tries to connect.
  defp validate_connect_opts!(opts) do
    case Keyword.fetch(opts, :connect_opts) do
      :error ->
        :ok

      {:ok, connect_opts} when is_list(connect_opts) ->
        validate_bool!(connect_opts, :tcp_nodelay)
        validate_bool!(connect_opts, :tcp_keepalive)
        validate_optional_pos_integer!(connect_opts, :tcp_sndbuf)
        validate_optional_pos_integer!(connect_opts, :tcp_rcvbuf)
        validate_pos_integer!(connect_opts, :connect_timeout_ms)
        validate_pos_integer!(connect_opts, :info_timeout)
        validate_tls_opts!(connect_opts)
        :ok

      {:ok, value} ->
        raise ArgumentError,
              "Aerospike.Supervisor: :connect_opts must be a keyword list, got #{inspect(value)}"
    end
  end

  # TLS-specific opt shape checks. The transport itself also raises on
  # bad values, but catching them here gives `start_link/1` a synchronous
  # failure point — operators see a typo in their config before the first
  # pool worker tries to connect.
  defp validate_tls_opts!(connect_opts) do
    validate_string_or_nil!(connect_opts, :tls_name)
    validate_path_or_nil!(connect_opts, :tls_cacertfile)
    validate_path_or_nil!(connect_opts, :tls_certfile)
    validate_path_or_nil!(connect_opts, :tls_keyfile)
    validate_tls_verify!(connect_opts)
    validate_tls_cert_key_pair!(connect_opts)
    :ok
  end

  defp validate_string_or_nil!(opts, key) do
    case Keyword.fetch(opts, key) do
      :error -> :ok
      {:ok, nil} -> :ok
      {:ok, value} when is_binary(value) -> :ok
      {:ok, value} -> raise_string!(key, value)
    end
  end

  defp validate_path_or_nil!(opts, key) do
    case Keyword.fetch(opts, key) do
      :error -> :ok
      {:ok, nil} -> :ok
      {:ok, value} when is_binary(value) -> :ok
      {:ok, value} -> raise_string!(key, value)
    end
  end

  defp validate_tls_verify!(opts) do
    case Keyword.fetch(opts, :tls_verify) do
      :error -> :ok
      {:ok, :verify_peer} -> :ok
      {:ok, :verify_none} -> :ok
      {:ok, value} -> raise_tls_verify!(value)
    end
  end

  # `:tls_certfile` and `:tls_keyfile` ship together or not at all —
  # mTLS requires both; standard TLS sets neither. A half-configured
  # pair is a common copy-paste mistake worth rejecting synchronously.
  defp validate_tls_cert_key_pair!(opts) do
    cert = Keyword.get(opts, :tls_certfile)
    key = Keyword.get(opts, :tls_keyfile)

    case {cert, key} do
      {nil, nil} ->
        :ok

      {c, k} when is_binary(c) and is_binary(k) ->
        :ok

      _ ->
        raise ArgumentError,
              "Aerospike.Supervisor: connect_opts :tls_certfile and :tls_keyfile " <>
                "must both be strings or both be absent, " <>
                "got certfile=#{inspect(cert)} keyfile=#{inspect(key)}"
    end
  end

  defp raise_string!(key, value) do
    raise ArgumentError,
          "Aerospike.Supervisor: connect_opts #{inspect(key)} must be a string or nil, " <>
            "got #{inspect(value)}"
  end

  defp raise_tls_verify!(value) do
    raise ArgumentError,
          "Aerospike.Supervisor: connect_opts :tls_verify must be :verify_peer or " <>
            ":verify_none, got #{inspect(value)}"
  end

  defp validate_bool!(opts, key) do
    case Keyword.fetch(opts, key) do
      :error ->
        :ok

      {:ok, value} when is_boolean(value) ->
        :ok

      {:ok, value} ->
        raise ArgumentError,
              "Aerospike.Supervisor: connect_opts #{inspect(key)} must be a boolean, " <>
                "got #{inspect(value)}"
    end
  end

  defp validate_optional_pos_integer!(opts, key) do
    case Keyword.fetch(opts, key) do
      :error -> :ok
      {:ok, nil} -> :ok
      {:ok, value} when is_integer(value) and value > 0 -> :ok
      {:ok, value} -> raise_pos_integer!(key, value)
    end
  end
end
