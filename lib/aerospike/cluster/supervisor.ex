defmodule Aerospike.Cluster.Supervisor do
  @moduledoc """
  Top-level supervisor for one named Aerospike cluster.

  Starts the table owner, per-node pool supervisor, partition-map writer,
  and tend-cycle worker under `rest_for_one` so the blast radius of a crash
  matches which process owns what state.

  Crash semantics under `rest_for_one`:

    * Tend-cycle crash → only the tend-cycle worker restarts. ETS tables
      survive, the pool supervisor and writer survive, and the restarted
      worker rehydrates its `:ready` flag from the `:meta` table.
    * Writer crash → the tend-cycle worker also restarts (it is after the
      writer). The
      next tend-cycle write would target a dead writer anyway; taking
      the worker with it keeps the cycle consistent.
    * Pool-supervisor crash → writer and tend-cycle worker also restart.
    * Table-owner crash → the whole subtree restarts with fresh tables.

  This module only supervises. It does not start pools, does not run
  tend cycles, and does not own any application state.
  """

  alias Aerospike.Cluster.NodeSupervisor
  alias Aerospike.Cluster.PartitionMapWriter
  alias Aerospike.Cluster.TableOwner
  alias Aerospike.Cluster.Tender
  alias Aerospike.Policy

  @typedoc "Authentication mode used during node login."
  @type auth_mode :: :internal | :external | :pki

  @typedoc "Cluster tend scheduling mode."
  @type tend_trigger :: :timer | :manual

  @typedoc """
  Transport connection option accepted inside startup `:connect_opts`.

  TCP transports use the timeout and socket tuning keys. TLS transports
  also use the `:tls_*` keys. Auth credentials are configured as top-level
  startup options; the Tender forwards derived auth/session values to
  transports internally. Other atom keys are forwarded to custom
  transports and ignored by the built-in transports.
  """
  @type connect_option ::
          {:connect_timeout_ms, pos_integer()}
          | {:info_timeout, pos_integer()}
          | {:login_timeout_ms, pos_integer()}
          | {:tcp_nodelay, boolean()}
          | {:tcp_keepalive, boolean()}
          | {:tcp_sndbuf, pos_integer() | nil}
          | {:tcp_rcvbuf, pos_integer() | nil}
          | {:tls_name, String.t() | nil}
          | {:tls_cacertfile, Path.t() | nil}
          | {:tls_certfile, Path.t() | nil}
          | {:tls_keyfile, Path.t() | nil}
          | {:tls_verify, :verify_peer | :verify_none}
          | {:tls_opts, keyword()}
          | {atom(), term()}

  @typedoc "Keyword list of transport connection options."
  @type connect_options :: [connect_option()]

  @typedoc """
  Startup option accepted by `Aerospike.start_link/1` and
  `Aerospike.child_spec/1`.

    * `:name` — atom used as the cluster identity (required). Becomes
      the cluster process name, table prefix, and pool-supervisor name.
    * `:transport` — module implementing `Aerospike.Cluster.NodeTransport`
      (required).
    * `:hosts` — list of `"host:port"` or `"host"` seed strings
      (required, non-empty).
    * `:namespaces` — list of namespace strings the cluster must serve
      before `Aerospike.Cluster.ready?/1` returns `true` (required,
      non-empty).

  Pool-level knobs live at the top level of the keyword list because
  the pool supervisor, not the transport, applies them. TCP and TLS tuning
  knobs live inside `:connect_opts` because the transport owns socket setup.

  Pool and lifecycle keys:

    * `:pool_size` — workers per node. Positive integer.
    * `:min_connections_per_node` — warm connection target per node.
      Non-negative integer.
    * `:idle_timeout_ms` — idle worker eviction timeout in milliseconds.
      Positive integer.
    * `:max_idle_pings` — maximum idle workers dropped per verification
      cycle. Positive integer.
    * `:tend_interval_ms` — automatic tend period in milliseconds.
      Positive integer.
    * `:tend_trigger` — `:timer` or `:manual`.

  Breaker and command-default keys:

    * `:failure_threshold` — consecutive tend failures before node
      demotion. Non-negative integer.
    * `:circuit_open_threshold` — consecutive node failures before the
      breaker refuses commands. Non-negative integer.
    * `:max_concurrent_ops_per_node` — per-node in-flight plus queued
      command cap. Positive integer.
    * `:max_retries`, `:sleep_between_retries_ms`, `:replica_policy` —
      default retry policy. See `t:Aerospike.RetryPolicy.option/0`.
    * `:use_compression` — cluster-wide request compression opt-in.

  Discovery and identity keys:

    * `:use_services_alternate` — use alternate peer endpoints during
      discovery.
    * `:seed_only_cluster` — skip peer discovery and use only configured
      seeds.
    * `:cluster_name` — expected server cluster name.
    * `:application_id` — client application identity sent to supporting
      servers.

  `:user` and `:password` are cluster-wide session-login credentials. Both
  must be present together; neither present disables auth. PKI auth uses the
  TLS client certificate and must omit both.
  """
  @type option ::
          {:name, atom()}
          | {:transport, module()}
          | {:hosts, [String.t(), ...]}
          | {:namespaces, [String.t(), ...]}
          | {:connect_opts, connect_options()}
          | {:pool_size, pos_integer()}
          | {:min_connections_per_node, non_neg_integer()}
          | {:idle_timeout_ms, pos_integer()}
          | {:max_idle_pings, pos_integer()}
          | {:tend_interval_ms, pos_integer()}
          | {:tend_trigger, tend_trigger()}
          | {:failure_threshold, non_neg_integer()}
          | {:circuit_open_threshold, non_neg_integer()}
          | {:max_concurrent_ops_per_node, pos_integer()}
          | {:max_retries, non_neg_integer()}
          | {:sleep_between_retries_ms, non_neg_integer()}
          | {:replica_policy, Aerospike.RetryPolicy.replica_policy()}
          | {:use_compression, boolean()}
          | {:use_services_alternate, boolean()}
          | {:seed_only_cluster, boolean()}
          | {:cluster_name, String.t()}
          | {:application_id, String.t()}
          | {:auth_mode, auth_mode()}
          | {:user, String.t()}
          | {:password, String.t()}
          | {:login_timeout_ms, pos_integer()}

  @typedoc "Keyword list of startup options."
  @type options :: [option()]

  @doc """
  Returns the OTP child specification for one named Aerospike cluster.

  Accepted options are documented by `t:option/0`.
  """
  @spec child_spec(options()) :: Supervisor.child_spec()
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

  Accepted options are documented by `t:option/0`. `:name`,
  `:transport`, `:hosts`, and `:namespaces` are required.
  """
  @spec start_link(options()) :: Supervisor.on_start()
  def start_link(opts) when is_list(opts) do
    validated = validate!(opts)
    name = Keyword.fetch!(validated, :name)
    hosts = Keyword.fetch!(validated, :hosts)

    tender_opts =
      validated
      |> Keyword.drop([:tables, :node_supervisor, :writer, :hosts])
      |> Keyword.put(:seeds, parse_hosts!(hosts))
      |> Keyword.put(:node_supervisor, NodeSupervisor.sup_name(name))
      |> Keyword.put(:writer, PartitionMapWriter.via(name))

    children = [
      {TableOwner, name: name},
      {NodeSupervisor, name: name},
      %{
        id: {PartitionMapWriter, name},
        start: {__MODULE__, :start_writer, [name]},
        type: :worker,
        restart: :permanent,
        shutdown: 5_000
      },
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
  # Resolves TableOwner tables at supervisor init time. TableOwner is already
  # up at this point, so `tables/1` is a synchronous call on a live process.
  @spec start_writer(atom()) :: GenServer.on_start()
  def start_writer(name) do
    tables = TableOwner.tables(TableOwner.via(name))

    PartitionMapWriter.start_link(name: name, tables: tables)
  end

  @doc false
  # Resolves TableOwner tables at supervisor init time. TableOwner is already
  # up at this point, so `tables/1` is a synchronous call on a live process.
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

  @required_keys [:name, :transport, :hosts, :namespaces]

  defp validate!(opts) do
    Enum.each(@required_keys, fn key ->
      Keyword.has_key?(opts, key) or
        raise ArgumentError,
              "Aerospike.Cluster.Supervisor: missing required option #{inspect(key)}"
    end)

    name = Keyword.fetch!(opts, :name)

    is_atom(name) or
      raise ArgumentError,
            "Aerospike.Cluster.Supervisor: :name must be an atom, got #{inspect(name)}"

    transport = Keyword.fetch!(opts, :transport)

    is_atom(transport) or
      raise ArgumentError,
            "Aerospike.Cluster.Supervisor: :transport must be a module, got #{inspect(transport)}"

    hosts = Keyword.fetch!(opts, :hosts)

    (is_list(hosts) and hosts != []) or
      raise ArgumentError,
            "Aerospike.Cluster.Supervisor: :hosts must be a non-empty list of \"host:port\" strings"

    _ = parse_hosts!(hosts)

    namespaces = Keyword.fetch!(opts, :namespaces)

    (is_list(namespaces) and namespaces != []) or
      raise ArgumentError,
            "Aerospike.Cluster.Supervisor: :namespaces must be a non-empty list of strings"

    validate_namespaces!(namespaces)

    validate_pos_integer!(opts, :pool_size)
    validate_non_neg_integer!(opts, :min_connections_per_node)
    validate_min_connections!(opts)
    validate_pos_integer!(opts, :idle_timeout_ms)
    validate_pos_integer!(opts, :max_idle_pings)
    validate_pos_integer!(opts, :tend_interval_ms)
    validate_pos_integer!(opts, :login_timeout_ms)
    validate_non_neg_integer!(opts, :failure_threshold)
    validate_non_neg_integer!(opts, :circuit_open_threshold)
    validate_pos_integer!(opts, :max_concurrent_ops_per_node)
    validate_auth_opts!(opts)
    validate_auth_mode!(opts)
    validate_non_empty_string!(opts, :cluster_name)
    validate_non_empty_string!(opts, :application_id)
    validate_top_level_bool!(opts, :use_compression)
    validate_top_level_bool!(opts, :use_services_alternate)
    validate_top_level_bool!(opts, :seed_only_cluster)
    validate_tend_trigger!(opts)
    validate_cluster_policy!(opts)
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
              "Aerospike.Cluster.Supervisor: :user and :password must both be strings or both be absent"
    end
  end

  defp validate_auth_mode!(opts) do
    auth_mode = Keyword.get(opts, :auth_mode, :internal)

    auth_mode in [:internal, :external, :pki] or
      raise ArgumentError,
            "Aerospike.Cluster.Supervisor: :auth_mode must be :internal, :external, or :pki, " <>
              "got #{inspect(auth_mode)}"

    validate_auth_mode_transport!(opts, auth_mode)
    validate_auth_mode_credentials!(opts, auth_mode)
  end

  defp validate_auth_mode_transport!(opts, auth_mode) when auth_mode in [:external, :pki] do
    case Keyword.fetch!(opts, :transport) do
      Aerospike.Transport.Tls ->
        :ok

      transport ->
        raise ArgumentError,
              "Aerospike.Cluster.Supervisor: #{inspect(auth_mode)} auth requires Aerospike.Transport.Tls, " <>
                "got #{inspect(transport)}"
    end
  end

  defp validate_auth_mode_transport!(_opts, _auth_mode), do: :ok

  defp validate_auth_mode_credentials!(opts, :external) do
    if is_binary(Keyword.get(opts, :user)) and is_binary(Keyword.get(opts, :password)) do
      :ok
    else
      raise ArgumentError,
            "Aerospike.Cluster.Supervisor: :external auth requires :user and :password strings"
    end
  end

  defp validate_auth_mode_credentials!(opts, :pki) do
    case {Keyword.get(opts, :user), Keyword.get(opts, :password)} do
      {nil, nil} ->
        :ok

      _ ->
        raise ArgumentError,
              "Aerospike.Cluster.Supervisor: :pki auth uses the TLS client certificate; " <>
                ":user and :password must be absent"
    end
  end

  defp validate_auth_mode_credentials!(_opts, _auth_mode), do: :ok

  defp validate_min_connections!(opts) do
    min_connections = Keyword.get(opts, :min_connections_per_node)
    pool_size = Keyword.get(opts, :pool_size)

    cond do
      is_nil(min_connections) or is_nil(pool_size) ->
        :ok

      min_connections <= pool_size ->
        :ok

      true ->
        raise ArgumentError,
              "Aerospike.Cluster.Supervisor: :min_connections_per_node must be less than or equal to :pool_size"
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
              "Aerospike.Cluster.Supervisor: #{inspect(key)} must be a non-negative integer, " <>
                "got #{inspect(value)}"
    end
  end

  defp raise_pos_integer!(key, value) do
    raise ArgumentError,
          "Aerospike.Cluster.Supervisor: #{inspect(key)} must be a positive integer, " <>
            "got #{inspect(value)}"
  end

  defp parse_hosts!(hosts) do
    Enum.map(hosts, &parse_host!/1)
  end

  defp parse_host!(host_port) when is_binary(host_port) and host_port != "" do
    case String.split(host_port, ":", parts: 2) do
      [host, port] when host != "" ->
        {host, parse_host_port!(host_port, port)}

      [host] when host != "" ->
        {host, 3000}

      _ ->
        raise ArgumentError,
              "Aerospike.Cluster.Supervisor: each host must be a non-empty string in " <>
                "\"host:port\" or \"host\" form, got #{inspect(host_port)}"
    end
  end

  defp parse_host!(host_port) do
    raise ArgumentError,
          "Aerospike.Cluster.Supervisor: each host must be a non-empty string in " <>
            "\"host:port\" or \"host\" form, got #{inspect(host_port)}"
  end

  defp parse_host_port!(host_port, port) do
    case Integer.parse(port) do
      {value, ""} when value in 1..65_535 ->
        value

      _ ->
        raise ArgumentError,
              "Aerospike.Cluster.Supervisor: invalid host port in #{inspect(host_port)}"
    end
  end

  defp validate_namespaces!(namespaces) do
    Enum.each(namespaces, fn
      namespace when is_binary(namespace) and namespace != "" ->
        :ok

      namespace ->
        raise ArgumentError,
              "Aerospike.Cluster.Supervisor: each namespace must be a non-empty string, " <>
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
              "Aerospike.Cluster.Supervisor: #{inspect(key)} must be a boolean, got #{inspect(value)}"
    end
  end

  defp validate_non_empty_string!(opts, key) do
    case Keyword.fetch(opts, key) do
      :error ->
        :ok

      {:ok, value} when is_binary(value) and value != "" ->
        :ok

      {:ok, value} ->
        raise ArgumentError,
              "Aerospike.Cluster.Supervisor: #{inspect(key)} must be a non-empty string, " <>
                "got #{inspect(value)}"
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
              "Aerospike.Cluster.Supervisor: :tend_trigger must be :timer or :manual, " <>
                "got #{inspect(value)}"
    end
  end

  defp validate_cluster_policy!(opts) do
    _ = Policy.cluster_defaults(opts)
    :ok
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
        validate_pos_integer!(connect_opts, :login_timeout_ms)
        validate_tls_opts!(connect_opts)
        :ok

      {:ok, value} ->
        raise ArgumentError,
              "Aerospike.Cluster.Supervisor: :connect_opts must be a keyword list, got #{inspect(value)}"
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
              "Aerospike.Cluster.Supervisor: connect_opts :tls_certfile and :tls_keyfile " <>
                "must both be strings or both be absent, " <>
                "got certfile=#{inspect(cert)} keyfile=#{inspect(key)}"
    end
  end

  defp raise_string!(key, value) do
    raise ArgumentError,
          "Aerospike.Cluster.Supervisor: connect_opts #{inspect(key)} must be a string or nil, " <>
            "got #{inspect(value)}"
  end

  defp raise_tls_verify!(value) do
    raise ArgumentError,
          "Aerospike.Cluster.Supervisor: connect_opts :tls_verify must be :verify_peer or " <>
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
              "Aerospike.Cluster.Supervisor: connect_opts #{inspect(key)} must be a boolean, " <>
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
