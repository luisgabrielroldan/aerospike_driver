defmodule Aerospike do
  @moduledoc """
  Public API for the Aerospike Elixir client.

  For application-facing code, prefer defining `MyApp.Repo` with
  `use Aerospike.Repo` so day-to-day calls do not pass a connection handle
  explicitly. This module remains the canonical low-level facade when you need
  direct connection startup, routing, or the literal `conn`-first API.

  Start a named connection (supervision tree) with `start_link/1`, then pass
  the same `name` atom as the first argument to CRUD functions.

  ## Example

      {:ok, _pid} =
        Aerospike.start_link(
          name: :aero,
          hosts: ["127.0.0.1:3000"],
          pool_size: 4
        )

      key = Aerospike.key("test", "users", "user:1")
      :ok = Aerospike.put!(:aero, key, %{"name" => "Ada"})
      {:ok, record} = Aerospike.get(:aero, key)
      record.bins["name"] == "Ada"

      import Aerospike.Op
      {:ok, rec2} = Aerospike.operate(:aero, key, [put("status", "ok"), get("name")])
      rec2.bins["name"] == "Ada"

      # Tuple key form (namespace, set, user_key)
      :ok = Aerospike.put!(:aero, {"test", "users", "user:2"}, %{"name" => "Grace"})
      {:ok, rec3} = Aerospike.get(:aero, {"test", "users", "user:2"})
      rec3.bins["name"] == "Grace"

      Aerospike.close(:aero)

  ## Key Input Forms

  Single-record APIs accept keys in either form:

  * `%Aerospike.Key{}`
  * `{namespace, set, user_key}` tuple

  Tuple keys are a convenience for user-key flows. For digest-only workflows,
  call `key_digest/3` explicitly.

  """

  alias Aerospike.Admin
  alias Aerospike.Batch
  alias Aerospike.BatchOps
  alias Aerospike.CRUD
  alias Aerospike.Error
  alias Aerospike.ExecuteTask
  alias Aerospike.Exp
  alias Aerospike.IndexTask
  alias Aerospike.Key
  alias Aerospike.Page
  alias Aerospike.Policy
  alias Aerospike.Privilege
  alias Aerospike.Protocol.AsmMsg.Operation
  alias Aerospike.Protocol.OperateFlags
  alias Aerospike.Query
  alias Aerospike.RegisterTask
  alias Aerospike.Role
  alias Aerospike.Scan
  alias Aerospike.ScanOps
  alias Aerospike.Supervisor, as: AeroSupervisor
  alias Aerospike.Txn
  alias Aerospike.TxnRoll
  alias Aerospike.UDF
  alias Aerospike.User

  @dialyzer {:nowarn_function,
             [
               {:batch_get_header!, 2},
               {:batch_get_header!, 3},
               {:batch_get_operate!, 3},
               {:batch_get_operate!, 4},
               {:batch_delete!, 2},
               {:batch_delete!, 3},
               {:batch_udf!, 5},
               {:batch_udf!, 6},
               {:query_stream_node!, 3},
               {:query_stream_node!, 4},
               {:query_all_node!, 3},
               {:query_all_node!, 4},
               {:query_count_node!, 3},
               {:query_count_node!, 4},
               {:query_page_node!, 3},
               {:query_page_node!, 4},
               {:scan_stream_node!, 3},
               {:scan_stream_node!, 4},
               {:scan_all_node!, 3},
               {:scan_all_node!, 4},
               {:scan_count_node!, 3},
               {:scan_count_node!, 4},
               {:scan_page_node!, 3},
               {:scan_page_node!, 4}
             ]}

  @typedoc """
  Named connection handle: currently an atom (`:name` from `start_link/1`).

  Internal routing and ETS table names assume atom registration today. Broader
  `GenServer.server()` support may be added in a future phase.
  """
  @type conn :: atom()

  @typedoc """
  One cluster node as returned by `nodes/1`: display `name`, TCP `host`, and `port`.
  """
  @type node_info :: %{name: String.t(), host: String.t(), port: non_neg_integer()}

  @typedoc """
  Cluster node identifier as returned by `node_names/1` or the `:name` field from `nodes/1`.
  """
  @type node_name :: String.t()

  @typedoc "Security user metadata returned by `query_user/3` and `query_users/2`."
  @type user_info :: User.t()

  @typedoc "Security role metadata returned by `query_role/3` and `query_roles/2`."
  @type role_info :: Role.t()

  @typedoc "Security privilege descriptor used in role operations."
  @type privilege :: Privilege.t()

  @typedoc "Server-side UDF package metadata returned by `list_udfs/2`."
  @type udf_info :: UDF.t()

  @typedoc "Background query task returned by `query_execute/4` and `query_udf/6`."
  @type execute_task :: ExecuteTask.t()

  @typedoc "One aggregate result emitted by `query_aggregate/6`."
  @type aggregate_value :: term()

  @typedoc "Secondary-index data type used when creating an index."
  @type index_data_type :: :numeric | :string | :geo2dsphere

  @typedoc "Top-level collection selector for list/map secondary indexes."
  @type index_collection :: :list | :mapkeys | :mapvalues

  @doc """
  Returns a child specification for supervision trees.

  Options are validated with `NimbleOptions`; invalid options raise `ArgumentError`.

  ## Options

  See `start_link/1`.

  ## Example

      children = [
        {Aerospike, name: :aero, hosts: ["127.0.0.1:3000"]}
      ]

  """
  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(opts) when is_list(opts), do: AeroSupervisor.child_spec(opts)

  @doc """
  Starts the client supervision tree under a registered supervisor name.

  Returns `{:ok, pid}` on success. If no seed is reachable at startup, the
  cluster retries on each tend cycle until a seed becomes available. Poll the
  `:cluster_ready` ETS flag to detect when the cluster is operational.

  ## Options

  * `:name` — required atom used as `conn` for all operations.
  * `:hosts` — required non-empty list of host strings (`"host:port"` or `"host"` for port 3000).
  * `:pool_size` — connections per node (default `10`).
  * `:pool_checkout_timeout` — pool checkout timeout in ms (default `5000`).
  * `:connect_timeout` — TCP connect timeout in ms (default `5000`).
  * `:tend_interval` — periodic cluster tend interval in ms (default `1000`).
  * `:recv_timeout` — receive timeout for protocol reads in ms (default `5000`).
  * `:max_error_rate` — per-node circuit-breaker threshold within the configured
    tend window (default `100`). Set to `0` to disable breaker logic.
  * `:error_rate_window` — circuit-breaker window size in tend ticks
    (default `1`).
  * `:auth_opts` — optional authentication keyword list.
  * `:tls` — when `true`, upgrades each node connection with TLS after TCP connect (default `false`).
  * `:tls_opts` — keyword list passed to `:ssl.connect/3` (certificates, verify, SNI, etc.; default `[]`).
    For non-IP hosts, `:server_name_indication` defaults to the hostname unless set in `:tls_opts`.
  * `:defaults` — policy defaults per command (`:write`, `:read`, `:delete`, `:exists`, `:touch`,
    `:operate`, `:batch`, `:scan`, `:query`).

  ## TLS example

      Aerospike.start_link(
        name: :aero_tls,
        hosts: ["aerospike.example.com:4333"],
        tls: true,
        tls_opts: [
          verify: :verify_peer,
          cacertfile: "/etc/ssl/certs/ca-certificates.crt"
        ]
      )

  ## Circuit breaker example

      Aerospike.start_link(
        name: :aero_cb,
        hosts: ["127.0.0.1:3000"],
        max_error_rate: 50,
        error_rate_window: 5
      )

  The breaker uses fixed-threshold semantics per node: requests are rejected
  with `:max_error_rate` once a node reaches `max_error_rate` errors inside the
  window, and counters reset every `error_rate_window` tend ticks.

  Telemetry events:

  * `[:aerospike, :circuit_breaker, :increment]`
  * `[:aerospike, :circuit_breaker, :reject]`
  * `[:aerospike, :circuit_breaker, :reset]`

  """
  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts) when is_list(opts), do: AeroSupervisor.start_link(opts)

  @doc """
  Stops the supervision tree for `conn` (the `:name` given at startup).

  `timeout` is passed to `Supervisor.stop/3` (default `15_000` ms).
  """
  @spec close(conn, timeout :: non_neg_integer()) :: :ok
  def close(conn, timeout \\ 15_000)
      when is_atom(conn) and is_integer(timeout) and timeout >= 0 do
    _ = Supervisor.stop(AeroSupervisor.sup_name(conn), :normal, timeout)
    :ok
  end

  @doc """
  Builds a key from namespace, set, and user key (string or int64 integer).

  Delegates to `Aerospike.Key.new/3`.
  """
  @spec key(String.t(), String.t(), String.t() | integer()) :: Key.t()
  defdelegate key(namespace, set, user_key), to: Key, as: :new

  @doc """
  Builds a key from namespace, set, and an existing 20-byte digest.

  Delegates to `Aerospike.Key.from_digest/3`.
  """
  @spec key_digest(String.t(), String.t(), <<_::160>>) :: Key.t()
  defdelegate key_digest(namespace, set, digest), to: Key, as: :from_digest

  @doc """
  Writes bins for the given key.

  Accepts `%Aerospike.Key{}` or `{namespace, set, user_key}` tuple keys.

  Per-call `opts` are merged over connection `defaults` (`Keyword.merge/2`).

  ## Options

  Write policy options: `:ttl`, `:timeout`, `:generation`, `:gen_policy`, `:exists`,
  `:send_key`, `:durable_delete`, `:pool_checkout_timeout`, `:replica`.

  ## Example

      key = Aerospike.key("test", "users", "user:1")
      :ok = Aerospike.put(:aero, key, %{"name" => "Ada"})

      :ok = Aerospike.put(:aero, {"test", "users", "user:2"}, %{"name" => "Grace"})

  """
  @spec put(conn, Key.key_input(), Aerospike.Record.bins_input(), keyword()) ::
          :ok | {:error, Error.t()}
  def put(conn, key, bins, opts \\ []) when is_atom(conn) and is_map(bins) and is_list(opts) do
    with {:ok, key} <- coerce_key(key),
         {:ok, call_opts} <- Policy.validate_write(opts) do
      CRUD.put(conn, key, bins, call_opts)
    else
      {:error, %Error{} = e} ->
        {:error, e}

      {:error, %NimbleOptions.ValidationError{} = e} ->
        {:error,
         Error.from_result_code(:parameter_error, message: Policy.validation_error_message(e))}
    end
  end

  @doc """
  Same as `put/4` but returns `:ok` or raises `Aerospike.Error`.
  """
  @spec put!(conn, Key.key_input(), Aerospike.Record.bins_input(), keyword()) :: :ok
  def put!(conn, key, bins, opts \\ []) when is_atom(conn) and is_map(bins) and is_list(opts) do
    case put(conn, key, bins, opts) do
      :ok -> :ok
      {:error, %Error{} = e} -> raise e
    end
  end

  @doc """
  Reads a record for the key.

  Accepts tuple keys; see `put/4`.

  ## Options

  Read policy: `:timeout`, `:bins`, `:header_only`, `:pool_checkout_timeout`, `:replica`.
  """
  @spec get(conn, Key.key_input(), keyword()) :: {:ok, Aerospike.Record.t()} | {:error, Error.t()}
  def get(conn, key, opts \\ []) when is_atom(conn) and is_list(opts) do
    with {:ok, key} <- coerce_key(key),
         {:ok, call_opts} <- Policy.validate_read(opts) do
      CRUD.get(conn, key, call_opts)
    else
      {:error, %Error{} = e} ->
        {:error, e}

      {:error, %NimbleOptions.ValidationError{} = e} ->
        {:error,
         Error.from_result_code(:parameter_error, message: Policy.validation_error_message(e))}
    end
  end

  @doc """
  Same as `get/3` but returns the record or raises `Aerospike.Error`.
  """
  @spec get!(conn, Key.key_input(), keyword()) :: Aerospike.Record.t()
  def get!(conn, key, opts \\ []) when is_atom(conn) and is_list(opts) do
    case get(conn, key, opts) do
      {:ok, record} -> record
      {:error, %Error{} = e} -> raise e
    end
  end

  @doc """
  Deletes the record. Returns `{:ok, true}` if a record was removed, `{:ok, false}` if absent.

  Accepts tuple keys; see `put/4`.

  ## Options

  `:timeout`, `:durable_delete`, `:pool_checkout_timeout`, `:replica`.
  """
  @spec delete(conn, Key.key_input(), keyword()) :: {:ok, boolean()} | {:error, Error.t()}
  def delete(conn, key, opts \\ []) when is_atom(conn) and is_list(opts) do
    with {:ok, key} <- coerce_key(key),
         {:ok, call_opts} <- Policy.validate_delete(opts) do
      CRUD.delete(conn, key, call_opts)
    else
      {:error, %Error{} = e} ->
        {:error, e}

      {:error, %NimbleOptions.ValidationError{} = e} ->
        {:error,
         Error.from_result_code(:parameter_error, message: Policy.validation_error_message(e))}
    end
  end

  @doc """
  Same as `delete/3` but returns the boolean or raises `Aerospike.Error`.
  """
  @spec delete!(conn, Key.key_input(), keyword()) :: boolean()
  def delete!(conn, key, opts \\ []) when is_atom(conn) and is_list(opts) do
    case delete(conn, key, opts) do
      {:ok, deleted?} -> deleted?
      {:error, %Error{} = e} -> raise e
    end
  end

  @doc """
  Returns whether a record exists for the key.

  Accepts tuple keys; see `put/4`.

  ## Options

  `:timeout`, `:pool_checkout_timeout`, `:replica`.
  """
  @spec exists(conn, Key.key_input(), keyword()) :: {:ok, boolean()} | {:error, Error.t()}
  def exists(conn, key, opts \\ []) when is_atom(conn) and is_list(opts) do
    with {:ok, key} <- coerce_key(key),
         {:ok, call_opts} <- Policy.validate_exists(opts) do
      CRUD.exists(conn, key, call_opts)
    else
      {:error, %Error{} = e} ->
        {:error, e}

      {:error, %NimbleOptions.ValidationError{} = e} ->
        {:error,
         Error.from_result_code(:parameter_error, message: Policy.validation_error_message(e))}
    end
  end

  @doc """
  Same as `exists/3` but returns the boolean or raises `Aerospike.Error`.
  """
  @spec exists!(conn, Key.key_input(), keyword()) :: boolean()
  def exists!(conn, key, opts \\ []) when is_atom(conn) and is_list(opts) do
    case exists(conn, key, opts) do
      {:ok, exists?} -> exists?
      {:error, %Error{} = e} -> raise e
    end
  end

  @doc """
  Refreshes TTL without changing bins.

  Accepts tuple keys; see `put/4`.

  ## Options

  `:ttl`, `:timeout`, `:pool_checkout_timeout`, `:replica`.
  """
  @spec touch(conn, Key.key_input(), keyword()) :: :ok | {:error, Error.t()}
  def touch(conn, key, opts \\ []) when is_atom(conn) and is_list(opts) do
    with {:ok, key} <- coerce_key(key),
         {:ok, call_opts} <- Policy.validate_touch(opts) do
      CRUD.touch(conn, key, call_opts)
    else
      {:error, %Error{} = e} ->
        {:error, e}

      {:error, %NimbleOptions.ValidationError{} = e} ->
        {:error,
         Error.from_result_code(:parameter_error, message: Policy.validation_error_message(e))}
    end
  end

  @doc """
  Same as `touch/3` but returns `:ok` or raises `Aerospike.Error`.
  """
  @spec touch!(conn, Key.key_input(), keyword()) :: :ok
  def touch!(conn, key, opts \\ []) when is_atom(conn) and is_list(opts) do
    case touch(conn, key, opts) do
      :ok -> :ok
      {:error, %Error{} = e} -> raise e
    end
  end

  @doc """
  Runs multiple read/write operations on one record in a single atomic round-trip.

  Accepts tuple keys; see `put/4`.

  Pass a list of operations from `Aerospike.Op`, `Aerospike.Op.List`, `Aerospike.Op.Map`, etc.

  ## Options

  Merges **read** and **write** policy keys: `:timeout`, `:ttl`, `:generation`, `:gen_policy`,
  `:exists`, `:send_key`, `:durable_delete`, `:respond_per_each_op`, `:pool_checkout_timeout`,
  `:replica`.
  """
  @spec operate(conn, Key.key_input(), [Aerospike.Op.t()], keyword()) ::
          {:ok, Aerospike.Record.t()} | {:error, Error.t()}
  def operate(conn, key, ops, opts \\ [])
      when is_atom(conn) and is_list(ops) and is_list(opts) do
    with {:ok, key} <- coerce_key(key),
         {:ok, call_opts} <- Policy.validate_operate(opts) do
      CRUD.operate(conn, key, ops, call_opts)
    else
      {:error, %Error{} = e} ->
        {:error, e}

      {:error, %NimbleOptions.ValidationError{} = e} ->
        {:error,
         Error.from_result_code(:parameter_error, message: Policy.validation_error_message(e))}
    end
  end

  @doc """
  Same as `operate/4` but returns `%Aerospike.Record{}` or raises `Aerospike.Error`.
  """
  @spec operate!(conn, Key.key_input(), [Aerospike.Op.t()], keyword()) :: Aerospike.Record.t()
  def operate!(conn, key, ops, opts \\ [])
      when is_atom(conn) and is_list(ops) and is_list(opts) do
    case operate(conn, key, ops, opts) do
      {:ok, record} -> record
      {:error, %Error{} = e} -> raise e
    end
  end

  @doc """
  Atomically adds integer deltas to bins.

  Accepts tuple keys; see `put/4`.

  If the record does not exist, Aerospike implicitly creates it — bins start at the
  added value. This makes `add` the idiomatic way to implement counters.

  ## Options

  Write policy options: `:ttl`, `:timeout`, `:generation`, `:gen_policy`, `:exists`,
  `:send_key`, `:durable_delete`, `:pool_checkout_timeout`, `:replica`.

  ## Example

      :ok = Aerospike.add(:aero, key, %{"login_count" => 1, "bytes_used" => 256})

  """
  @spec add(conn, Key.key_input(), Aerospike.Record.bins_input(), keyword()) ::
          :ok | {:error, Error.t()}
  def add(conn, key, bins, opts \\ [])
      when is_atom(conn) and is_map(bins) and is_list(opts) do
    with {:ok, key} <- coerce_key(key),
         {:ok, call_opts} <- Policy.validate_write(opts) do
      CRUD.add(conn, key, bins, call_opts)
    else
      {:error, %Error{} = e} ->
        {:error, e}

      {:error, %NimbleOptions.ValidationError{} = e} ->
        {:error,
         Error.from_result_code(:parameter_error, message: Policy.validation_error_message(e))}
    end
  end

  @doc """
  Same as `add/4` but returns `:ok` or raises `Aerospike.Error`.
  """
  @spec add!(conn, Key.key_input(), Aerospike.Record.bins_input(), keyword()) :: :ok
  def add!(conn, key, bins, opts \\ [])
      when is_atom(conn) and is_map(bins) and is_list(opts) do
    case add(conn, key, bins, opts) do
      :ok -> :ok
      {:error, %Error{} = e} -> raise e
    end
  end

  @doc """
  Atomically appends string suffixes to bins.

  Accepts tuple keys; see `put/4`.

  If the record does not exist, Aerospike implicitly creates it — the bin value
  becomes the appended string (not appended to an empty string).

  ## Options

  Write policy options: `:ttl`, `:timeout`, `:generation`, `:gen_policy`, `:exists`,
  `:send_key`, `:durable_delete`, `:pool_checkout_timeout`, `:replica`.

  ## Example

      :ok = Aerospike.append(:aero, key, %{"greeting" => " world"})

  """
  @spec append(conn, Key.key_input(), Aerospike.Record.bins_input(), keyword()) ::
          :ok | {:error, Error.t()}
  def append(conn, key, bins, opts \\ [])
      when is_atom(conn) and is_map(bins) and is_list(opts) do
    with {:ok, key} <- coerce_key(key),
         {:ok, call_opts} <- Policy.validate_write(opts) do
      CRUD.append(conn, key, bins, call_opts)
    else
      {:error, %Error{} = e} ->
        {:error, e}

      {:error, %NimbleOptions.ValidationError{} = e} ->
        {:error,
         Error.from_result_code(:parameter_error, message: Policy.validation_error_message(e))}
    end
  end

  @doc """
  Same as `append/4` but returns `:ok` or raises `Aerospike.Error`.
  """
  @spec append!(conn, Key.key_input(), Aerospike.Record.bins_input(), keyword()) :: :ok
  def append!(conn, key, bins, opts \\ [])
      when is_atom(conn) and is_map(bins) and is_list(opts) do
    case append(conn, key, bins, opts) do
      :ok -> :ok
      {:error, %Error{} = e} -> raise e
    end
  end

  @doc """
  Atomically prepends string prefixes to bins.

  Accepts tuple keys; see `put/4`.

  If the record does not exist, Aerospike implicitly creates it — the bin value
  becomes the prepended string (not prepended to an empty string).

  ## Options

  Write policy options: `:ttl`, `:timeout`, `:generation`, `:gen_policy`, `:exists`,
  `:send_key`, `:durable_delete`, `:pool_checkout_timeout`, `:replica`.

  ## Example

      :ok = Aerospike.prepend(:aero, key, %{"greeting" => "hello "})

  """
  @spec prepend(conn, Key.key_input(), Aerospike.Record.bins_input(), keyword()) ::
          :ok | {:error, Error.t()}
  def prepend(conn, key, bins, opts \\ [])
      when is_atom(conn) and is_map(bins) and is_list(opts) do
    with {:ok, key} <- coerce_key(key),
         {:ok, call_opts} <- Policy.validate_write(opts) do
      CRUD.prepend(conn, key, bins, call_opts)
    else
      {:error, %Error{} = e} ->
        {:error, e}

      {:error, %NimbleOptions.ValidationError{} = e} ->
        {:error,
         Error.from_result_code(:parameter_error, message: Policy.validation_error_message(e))}
    end
  end

  @doc """
  Same as `prepend/4` but returns `:ok` or raises `Aerospike.Error`.
  """
  @spec prepend!(conn, Key.key_input(), Aerospike.Record.bins_input(), keyword()) :: :ok
  def prepend!(conn, key, bins, opts \\ [])
      when is_atom(conn) and is_map(bins) and is_list(opts) do
    case prepend(conn, key, bins, opts) do
      :ok -> :ok
      {:error, %Error{} = e} -> raise e
    end
  end

  @batch_read_opts [:bins, :header_only, :read_touch_ttl_percent]

  @doc """
  Reads multiple keys in one round-trip per server node.

  Returns `{:ok, records}` in **key order**; missing keys appear as `nil`.

  ## Options

  * Batch: `:timeout`, `:pool_checkout_timeout`, `:replica` (`:master`, `:sequence`, `:any`, or a
    non-negative replica index), `:respond_all_keys`, `:filter` (`Aerospike.Exp.from_wire/1`)
  * Read: `:bins`, `:header_only`, `:read_touch_ttl_percent` (omit `:bins` to read all bins;
    `:bins` must not be an empty list)

  ## Examples

      keys = Enum.map(1..3, &Aerospike.key("test", "users", "user:\#{&1}"))

      {:ok, records} = Aerospike.batch_get(:aero, keys)
      # records: [%Aerospike.Record{} | nil, ...] — aligned with keys

      # Project specific bins
      {:ok, records} = Aerospike.batch_get(:aero, keys, bins: ["name", "age"])

  """
  @spec batch_get(conn, [Key.key_input()], keyword()) ::
          {:ok, [Aerospike.Record.t() | nil]} | {:error, Error.t()}
  def batch_get(conn, keys, opts \\ []) when is_atom(conn) and is_list(keys) and is_list(opts) do
    # `Keyword.split/2` returns `{taken_for_these_keys, rest}`.
    {read_kw, batch_kw} = Keyword.split(opts, @batch_read_opts)

    with {:ok, keys} <- coerce_keys(keys),
         {:ok, bopts} <- Policy.validate_batch(batch_kw),
         {:ok, ropts} <- Policy.validate_read(read_kw) do
      BatchOps.batch_get(conn, keys, Keyword.merge(bopts, ropts))
    else
      {:error, %Error{} = e} ->
        {:error, e}

      {:error, %NimbleOptions.ValidationError{} = e} ->
        {:error,
         Error.from_result_code(:parameter_error, message: Policy.validation_error_message(e))}
    end
  end

  @doc """
  Same as `batch_get/3` but returns the list or raises `Aerospike.Error`.

  ## Example

      records = Aerospike.batch_get!(:aero, keys, bins: ["name"])

  """
  @spec batch_get!(conn, [Key.key_input()], keyword()) :: [Aerospike.Record.t() | nil]
  def batch_get!(conn, keys, opts \\ []) when is_atom(conn) and is_list(keys) and is_list(opts) do
    case batch_get(conn, keys, opts) do
      {:ok, recs} -> recs
      {:error, %Error{} = e} -> raise e
    end
  end

  @doc """
  Checks existence for multiple keys in one round-trip per node.

  Returns `{:ok, booleans}` aligned with `keys`.

  ## Options

  `:timeout`, `:pool_checkout_timeout`, `:replica` (`:master`, `:sequence`, `:any`, or index),
  `:respond_all_keys`, `:filter` (`Aerospike.Exp.from_wire/1`)

  ## Example

      keys = [key1, key2, key3]
      {:ok, [true, false, true]} = Aerospike.batch_exists(:aero, keys)

  """
  @spec batch_exists(conn, [Key.key_input()], keyword()) ::
          {:ok, [boolean()]} | {:error, Error.t()}
  def batch_exists(conn, keys, opts \\ [])
      when is_atom(conn) and is_list(keys) and is_list(opts) do
    with {:ok, keys} <- coerce_keys(keys),
         {:ok, bopts} <- Policy.validate_batch(opts) do
      BatchOps.batch_exists(conn, keys, bopts)
    else
      {:error, %Error{} = e} ->
        {:error, e}

      {:error, %NimbleOptions.ValidationError{} = e} ->
        {:error,
         Error.from_result_code(:parameter_error, message: Policy.validation_error_message(e))}
    end
  end

  @doc """
  Same as `batch_exists/3` but returns the list or raises `Aerospike.Error`.

  ## Example

      [true, false] = Aerospike.batch_exists!(:aero, [key1, key2])

  """
  @spec batch_exists!(conn, [Key.key_input()], keyword()) :: [boolean()]
  def batch_exists!(conn, keys, opts \\ [])
      when is_atom(conn) and is_list(keys) and is_list(opts) do
    case batch_exists(conn, keys, opts) do
      {:ok, xs} -> xs
      {:error, %Error{} = e} -> raise e
    end
  end

  @doc """
  Reads record headers for multiple keys in one batch call.

  This dedicated wrapper is accepted for Phase 4 because the capability already
  exists under `batch_get(..., header_only: true)`, but the narrower name is
  easier to discover and makes the intended return shape explicit.

  Returns records aligned with `keys`; missing keys appear as `nil`.

  `batch_get_header/3` rejects `:bins` and explicit `:header_only`; the wrapper
  always enforces header-only semantics itself.
  """
  @spec batch_get_header(conn, [Key.key_input()], keyword()) ::
          {:ok, [Aerospike.Record.t() | nil]} | {:error, Error.t()}
  def batch_get_header(conn, keys, opts \\ [])
      when is_atom(conn) and is_list(keys) and is_list(opts) do
    with :ok <- validate_batch_get_header_opts(opts) do
      batch_get(conn, keys, Keyword.put(opts, :header_only, true))
    end
  end

  @doc """
  Same as `batch_get_header/3` but returns the list or raises `Aerospike.Error`.
  """
  @spec batch_get_header!(conn, [Key.key_input()], keyword()) :: [Aerospike.Record.t() | nil]
  def batch_get_header!(conn, keys, opts \\ [])
      when is_atom(conn) and is_list(keys) and is_list(opts) do
    case batch_get_header(conn, keys, opts) do
      {:ok, recs} -> recs
      {:error, %Error{} = e} -> raise e
    end
  end

  @doc """
  Executes one shared read-only operate list against multiple keys in one batch call.

  This is the accepted homogeneous/read-focused companion to `batch_operate/3`.
  Mixed read/write operate flows remain the responsibility of
  `Aerospike.Batch.operate/3` plus `batch_operate/3`.

  Returns records in key order, with `nil` for missing keys.

  The shared `ops` list must be non-empty and read-only. Missing keys, filtered
  reads, and missing projected bins normalize to `nil`, matching the existing
  `batch_get/3` shape.
  """
  @spec batch_get_operate(conn, [Key.key_input()], [Aerospike.Op.t()], keyword()) ::
          {:ok, [Aerospike.Record.t() | nil]} | {:error, Error.t()}
  def batch_get_operate(conn, keys, ops, opts \\ [])
      when is_atom(conn) and is_list(keys) and is_list(ops) and is_list(opts) do
    with {:ok, keys} <- coerce_keys(keys),
         :ok <- validate_batch_get_operate_ops(ops),
         {:ok, bopts} <- Policy.validate_batch(opts),
         {:ok, results} <- BatchOps.batch_operate(conn, batch_operate_ops(keys, ops), bopts) do
      normalize_batch_get_operate_results(results)
    else
      {:error, %Error{} = e} ->
        {:error, e}

      {:error, %NimbleOptions.ValidationError{} = e} ->
        {:error,
         Error.from_result_code(:parameter_error, message: Policy.validation_error_message(e))}
    end
  end

  @doc """
  Same as `batch_get_operate/4` but returns the list or raises `Aerospike.Error`.
  """
  @spec batch_get_operate!(conn, [Key.key_input()], [Aerospike.Op.t()], keyword()) ::
          [Aerospike.Record.t() | nil]
  def batch_get_operate!(conn, keys, ops, opts \\ [])
      when is_atom(conn) and is_list(keys) and is_list(ops) and is_list(opts) do
    case batch_get_operate(conn, keys, ops, opts) do
      {:ok, recs} -> recs
      {:error, %Error{} = e} -> raise e
    end
  end

  @doc """
  Deletes multiple keys with one uniform delete intent.

  The accepted return shape is a list of `%Aerospike.BatchResult{}` structs in
  input order. A plain boolean list would hide real per-key failure detail, and
  the current batch response model does not preserve enough information to
  collapse every delete outcome honestly into `true` or `false`.

  """
  @spec batch_delete(conn, [Key.key_input()], keyword()) ::
          {:ok, [Aerospike.BatchResult.t()]} | {:error, Error.t()}
  def batch_delete(conn, keys, opts \\ [])
      when is_atom(conn) and is_list(keys) and is_list(opts) do
    with {:ok, keys} <- coerce_keys(keys),
         {:ok, bopts} <- Policy.validate_batch(opts) do
      BatchOps.batch_operate(conn, Enum.map(keys, &Batch.delete/1), bopts)
    else
      {:error, %Error{} = e} ->
        {:error, e}

      {:error, %NimbleOptions.ValidationError{} = e} ->
        {:error,
         Error.from_result_code(:parameter_error, message: Policy.validation_error_message(e))}
    end
  end

  @doc """
  Same as `batch_delete/3` but returns results or raises `Aerospike.Error`.
  """
  @spec batch_delete!(conn, [Key.key_input()], keyword()) :: [Aerospike.BatchResult.t()]
  def batch_delete!(conn, keys, opts \\ [])
      when is_atom(conn) and is_list(keys) and is_list(opts) do
    case batch_delete(conn, keys, opts) do
      {:ok, results} -> results
      {:error, %Error{} = e} -> raise e
    end
  end

  @doc """
  Executes the same record UDF against multiple keys in one batch call.

  The accepted public name is `batch_udf/6`, not `batch_execute/6`. The UDF
  intent is more explicit, and the library already uses `apply_udf/6`,
  `query_udf/6`, and `Aerospike.Batch.udf/5` for this concept.

  Return values stay in `%Aerospike.BatchResult{}` form because UDF responses
  can carry per-key success or failure details that do not compress cleanly into
  a plain homogeneous list.

  """
  @spec batch_udf(conn, [Key.key_input()], String.t(), String.t(), list(), keyword()) ::
          {:ok, [Aerospike.BatchResult.t()]} | {:error, Error.t()}
  def batch_udf(conn, keys, package, function, args, opts \\ [])
      when is_atom(conn) and is_list(keys) and is_binary(package) and is_binary(function) and
             is_list(args) and is_list(opts) do
    with {:ok, keys} <- coerce_keys(keys),
         {:ok, bopts} <- Policy.validate_batch(opts) do
      batch_ops = Enum.map(keys, &Batch.udf(&1, package, function, args))
      BatchOps.batch_operate(conn, batch_ops, bopts)
    else
      {:error, %Error{} = e} ->
        {:error, e}

      {:error, %NimbleOptions.ValidationError{} = e} ->
        {:error,
         Error.from_result_code(:parameter_error, message: Policy.validation_error_message(e))}
    end
  end

  @doc """
  Same as `batch_udf/6` but returns results or raises `Aerospike.Error`.
  """
  @spec batch_udf!(conn, [Key.key_input()], String.t(), String.t(), list(), keyword()) ::
          [Aerospike.BatchResult.t()]
  def batch_udf!(conn, keys, package, function, args, opts \\ [])
      when is_atom(conn) and is_list(keys) and is_binary(package) and is_binary(function) and
             is_list(args) and is_list(opts) do
    case batch_udf(conn, keys, package, function, args, opts) do
      {:ok, results} -> results
      {:error, %Error{} = e} -> raise e
    end
  end

  @doc """
  Runs a heterogeneous batch built with `Aerospike.Batch`.

  Each input operation produces one `Aerospike.BatchResult` in the same position.

  ## Options

  `:timeout`, `:pool_checkout_timeout`, `:replica` (`:master`, `:sequence`, `:any`, or index),
  `:respond_all_keys`, `:filter` (`Aerospike.Exp.from_wire/1`)

  ## Example

      alias Aerospike.Batch

      {:ok, results} =
        Aerospike.batch_operate(:aero, [
          Batch.read(key1, bins: ["name"]),
          Batch.put(key2, %{"score" => 100}),
          Batch.delete(key3),
          Batch.operate(key4, [Aerospike.Op.add("hits", 1)])
        ])

      Enum.each(results, fn
        %Aerospike.BatchResult{status: :ok, record: rec} ->
          IO.inspect(rec, label: "success")

        %Aerospike.BatchResult{status: :error, error: err} ->
          IO.inspect(err, label: "failed")
      end)

  """
  @spec batch_operate(conn, [Batch.t()], keyword()) ::
          {:ok, [Aerospike.BatchResult.t()]} | {:error, Error.t()}
  def batch_operate(conn, ops, opts \\ [])
      when is_atom(conn) and is_list(ops) and is_list(opts) do
    case Policy.validate_batch(opts) do
      {:ok, bopts} ->
        BatchOps.batch_operate(conn, ops, bopts)

      {:error, %NimbleOptions.ValidationError{} = e} ->
        {:error,
         Error.from_result_code(:parameter_error, message: Policy.validation_error_message(e))}
    end
  end

  @doc """
  Same as `batch_operate/3` but returns results or raises `Aerospike.Error`.

  ## Example

      results = Aerospike.batch_operate!(:aero, [Batch.read(key1)])

  """
  @spec batch_operate!(conn, [Batch.t()], keyword()) :: [Aerospike.BatchResult.t()]
  def batch_operate!(conn, ops, opts \\ [])
      when is_atom(conn) and is_list(ops) and is_list(opts) do
    case batch_operate(conn, ops, opts) do
      {:ok, rs} -> rs
      {:error, %Error{} = e} -> raise e
    end
  end

  @doc """
  Returns a lazy `Stream` of records from a secondary-index query.

  This is the explicit query-read entry point for `%Aerospike.Query{}`.
  Unlike `query_aggregate/6`, it always yields `%Aerospike.Record{}` structs.

  ## Options

  Query policy: `:timeout`, `:pool_checkout_timeout`, `:replica`,
  `:max_concurrent_nodes` (0 = all nodes).
  """
  @spec query_stream(conn(), Query.t(), keyword()) :: {:ok, Enumerable.t()} | {:error, Error.t()}
  def query_stream(conn, %Query{} = query, opts \\ []) when is_atom(conn) and is_list(opts) do
    case validate_scan_query_opts(query, opts) do
      {:ok, call_opts} ->
        {:ok, ScanOps.stream(conn, query, call_opts)}

      {:error, %NimbleOptions.ValidationError{} = e} ->
        {:error,
         Error.from_result_code(:parameter_error, message: Policy.validation_error_message(e))}
    end
  end

  @doc """
  Same as `query_stream/3` but returns the stream or raises `Aerospike.Error`.
  """
  @spec query_stream!(conn(), Query.t(), keyword()) :: Enumerable.t()
  def query_stream!(conn, %Query{} = query, opts \\ []) when is_atom(conn) and is_list(opts) do
    case query_stream(conn, query, opts) do
      {:ok, stream} -> stream
      {:error, %Error{} = e} -> raise e
    end
  end

  @doc """
  Eagerly collects query records into a list.

  Requires `max_records` on the query builder.
  """
  @spec query_all(conn(), Query.t(), keyword()) ::
          {:ok, [Aerospike.Record.t()]} | {:error, Error.t()}
  def query_all(conn, %Query{} = query, opts \\ []) when is_atom(conn) and is_list(opts) do
    case validate_scan_query_opts(query, opts) do
      {:ok, call_opts} ->
        ScanOps.all(conn, query, call_opts)

      {:error, %NimbleOptions.ValidationError{} = e} ->
        {:error,
         Error.from_result_code(:parameter_error, message: Policy.validation_error_message(e))}
    end
  end

  @doc """
  Same as `query_all/3` but returns the list or raises `Aerospike.Error`.
  """
  @spec query_all!(conn(), Query.t(), keyword()) :: [Aerospike.Record.t()]
  def query_all!(conn, %Query{} = query, opts \\ []) when is_atom(conn) and is_list(opts) do
    case query_all(conn, query, opts) do
      {:ok, records} -> records
      {:error, %Error{} = e} -> raise e
    end
  end

  @doc """
  Counts records matching a secondary-index query.
  """
  @spec query_count(conn(), Query.t(), keyword()) ::
          {:ok, non_neg_integer()} | {:error, Error.t()}
  def query_count(conn, %Query{} = query, opts \\ []) when is_atom(conn) and is_list(opts) do
    case validate_scan_query_opts(query, opts) do
      {:ok, call_opts} ->
        ScanOps.count(conn, query, call_opts)

      {:error, %NimbleOptions.ValidationError{} = e} ->
        {:error,
         Error.from_result_code(:parameter_error, message: Policy.validation_error_message(e))}
    end
  end

  @doc """
  Same as `query_count/3` but returns the count or raises `Aerospike.Error`.
  """
  @spec query_count!(conn(), Query.t(), keyword()) :: non_neg_integer()
  def query_count!(conn, %Query{} = query, opts \\ []) when is_atom(conn) and is_list(opts) do
    case query_count(conn, query, opts) do
      {:ok, n} -> n
      {:error, %Error{} = e} -> raise e
    end
  end

  @doc """
  Returns one page of records from a query with cursor-based pagination.
  """
  @spec query_page(conn(), Query.t(), keyword()) :: {:ok, Page.t()} | {:error, Error.t()}
  def query_page(conn, %Query{} = query, opts \\ []) when is_atom(conn) and is_list(opts) do
    {cursor, opts2} = Keyword.pop(opts, :cursor)

    case validate_scan_query_opts(query, opts2) do
      {:ok, call_opts} ->
        opts3 = if cursor != nil, do: Keyword.put(call_opts, :cursor, cursor), else: call_opts
        ScanOps.page(conn, query, opts3)

      {:error, %NimbleOptions.ValidationError{} = e} ->
        {:error,
         Error.from_result_code(:parameter_error, message: Policy.validation_error_message(e))}
    end
  end

  @doc """
  Same as `query_page/3` but returns `%Aerospike.Page{}` or raises `Aerospike.Error`.
  """
  @spec query_page!(conn(), Query.t(), keyword()) :: Page.t()
  def query_page!(conn, %Query{} = query, opts \\ []) when is_atom(conn) and is_list(opts) do
    case query_page(conn, query, opts) do
      {:ok, page} -> page
      {:error, %Error{} = e} -> raise e
    end
  end

  @doc """
  Returns a lazy `Stream` of records from a query restricted to one named node.

  Callers obtain `node_name` from `node_names/1` or from the `:name` field in
  `nodes/1`. Missing or stale node names return a normal `%Aerospike.Error{}`
  rather than silently falling back to cluster-wide execution.

  Phase 4 intentionally limits node-targeted support to record-read query APIs.
  Background query execution, query UDF mutation, and aggregate-query node
  targeting remain deferred.
  """
  @spec query_stream_node(conn(), node_name(), Query.t(), keyword()) ::
          {:ok, Enumerable.t()} | {:error, Error.t()}
  def query_stream_node(conn, node_name, %Query{} = query, opts \\ [])
      when is_atom(conn) and is_binary(node_name) and is_list(opts) do
    case validate_scan_query_opts(query, opts) do
      {:ok, call_opts} ->
        {:ok, ScanOps.stream_node(conn, node_name, query, call_opts)}

      {:error, %NimbleOptions.ValidationError{} = e} ->
        {:error,
         Error.from_result_code(:parameter_error, message: Policy.validation_error_message(e))}
    end
  end

  @doc """
  Same as `query_stream_node/4` but returns the stream or raises `Aerospike.Error`.
  """
  @spec query_stream_node!(conn(), node_name(), Query.t(), keyword()) :: Enumerable.t()
  def query_stream_node!(conn, node_name, %Query{} = query, opts \\ [])
      when is_atom(conn) and is_binary(node_name) and is_list(opts) do
    case query_stream_node(conn, node_name, query, opts) do
      {:ok, stream} -> stream
      {:error, %Error{} = e} -> raise e
    end
  end

  @doc """
  Eagerly collects query records from one named node.
  """
  @spec query_all_node(conn(), node_name(), Query.t(), keyword()) ::
          {:ok, [Aerospike.Record.t()]} | {:error, Error.t()}
  def query_all_node(conn, node_name, %Query{} = query, opts \\ [])
      when is_atom(conn) and is_binary(node_name) and is_list(opts) do
    case validate_scan_query_opts(query, opts) do
      {:ok, call_opts} ->
        ScanOps.all_node(conn, node_name, query, call_opts)

      {:error, %NimbleOptions.ValidationError{} = e} ->
        {:error,
         Error.from_result_code(:parameter_error, message: Policy.validation_error_message(e))}
    end
  end

  @doc """
  Same as `query_all_node/4` but returns the list or raises `Aerospike.Error`.
  """
  @spec query_all_node!(conn(), node_name(), Query.t(), keyword()) :: [Aerospike.Record.t()]
  def query_all_node!(conn, node_name, %Query{} = query, opts \\ [])
      when is_atom(conn) and is_binary(node_name) and is_list(opts) do
    case query_all_node(conn, node_name, query, opts) do
      {:ok, records} -> records
      {:error, %Error{} = e} -> raise e
    end
  end

  @doc """
  Counts query matches on one named node.
  """
  @spec query_count_node(conn(), node_name(), Query.t(), keyword()) ::
          {:ok, non_neg_integer()} | {:error, Error.t()}
  def query_count_node(conn, node_name, %Query{} = query, opts \\ [])
      when is_atom(conn) and is_binary(node_name) and is_list(opts) do
    case validate_scan_query_opts(query, opts) do
      {:ok, call_opts} ->
        ScanOps.count_node(conn, node_name, query, call_opts)

      {:error, %NimbleOptions.ValidationError{} = e} ->
        {:error,
         Error.from_result_code(:parameter_error, message: Policy.validation_error_message(e))}
    end
  end

  @doc """
  Same as `query_count_node/4` but returns the count or raises `Aerospike.Error`.
  """
  @spec query_count_node!(conn(), node_name(), Query.t(), keyword()) :: non_neg_integer()
  def query_count_node!(conn, node_name, %Query{} = query, opts \\ [])
      when is_atom(conn) and is_binary(node_name) and is_list(opts) do
    case query_count_node(conn, node_name, query, opts) do
      {:ok, n} -> n
      {:error, %Error{} = e} -> raise e
    end
  end

  @doc """
  Returns one page of query results from one named node.
  """
  @spec query_page_node(conn(), node_name(), Query.t(), keyword()) ::
          {:ok, Page.t()} | {:error, Error.t()}
  def query_page_node(conn, node_name, %Query{} = query, opts \\ [])
      when is_atom(conn) and is_binary(node_name) and is_list(opts) do
    {cursor, opts2} = Keyword.pop(opts, :cursor)

    case validate_scan_query_opts(query, opts2) do
      {:ok, call_opts} ->
        opts3 = if cursor != nil, do: Keyword.put(call_opts, :cursor, cursor), else: call_opts
        ScanOps.page_node(conn, node_name, query, opts3)

      {:error, %NimbleOptions.ValidationError{} = e} ->
        {:error,
         Error.from_result_code(:parameter_error, message: Policy.validation_error_message(e))}
    end
  end

  @doc """
  Same as `query_page_node/4` but returns `%Aerospike.Page{}` or raises `Aerospike.Error`.
  """
  @spec query_page_node!(conn(), node_name(), Query.t(), keyword()) :: Page.t()
  def query_page_node!(conn, node_name, %Query{} = query, opts \\ [])
      when is_atom(conn) and is_binary(node_name) and is_list(opts) do
    case query_page_node(conn, node_name, query, opts) do
      {:ok, page} -> page
      {:error, %Error{} = e} -> raise e
    end
  end

  @doc """
  Returns metadata for all UDF packages registered in the cluster.

  ## Options

  Info policy: `:timeout`, `:pool_checkout_timeout`.
  """
  @spec list_udfs(conn(), keyword()) :: {:ok, [udf_info()]} | {:error, Error.t()}
  def list_udfs(conn, opts \\ []) when is_atom(conn) and is_list(opts) do
    case Policy.validate_info(opts) do
      {:ok, call_opts} ->
        Admin.list_udfs(conn, call_opts)

      {:error, %NimbleOptions.ValidationError{} = e} ->
        {:error,
         Error.from_result_code(:parameter_error, message: Policy.validation_error_message(e))}
    end
  end

  @doc """
  Same as `list_udfs/2` but returns the list or raises `Aerospike.Error`.
  """
  @spec list_udfs!(conn(), keyword()) :: [udf_info()]
  def list_udfs!(conn, opts \\ []) when is_atom(conn) and is_list(opts) do
    case list_udfs(conn, opts) do
      {:ok, udfs} -> udfs
      {:error, %Error{} = e} -> raise e
    end
  end

  @doc """
  Starts a background query that applies write operations to matching records.

  Returns an `%Aerospike.ExecuteTask{}` that can later be polled or awaited.
  """
  @spec query_execute(conn(), Query.t(), list(), keyword()) ::
          {:ok, execute_task()} | {:error, Error.t()}
  def query_execute(conn, %Query{} = query, ops, opts \\ [])
      when is_atom(conn) and is_list(ops) and is_list(opts) do
    with :ok <- validate_query_ops(ops),
         {:ok, call_opts} <- Policy.validate_query(opts) do
      ScanOps.query_execute(conn, query, ops, call_opts)
    else
      {:error, %Error{} = e} ->
        {:error, e}

      {:error, %NimbleOptions.ValidationError{} = e} ->
        {:error,
         Error.from_result_code(:parameter_error, message: Policy.validation_error_message(e))}
    end
  end

  @doc """
  Same as `query_execute/4` but returns the task or raises `Aerospike.Error`.
  """
  @spec query_execute!(conn(), Query.t(), list(), keyword()) :: execute_task()
  def query_execute!(conn, %Query{} = query, ops, opts \\ [])
      when is_atom(conn) and is_list(ops) and is_list(opts) do
    case query_execute(conn, query, ops, opts) do
      {:ok, task} -> task
      {:error, %Error{} = e} -> raise e
    end
  end

  @doc """
  Starts a background query that applies a record UDF to matching records.

  Returns an `%Aerospike.ExecuteTask{}` that can later be polled or awaited.
  """
  @spec query_udf(conn(), Query.t(), String.t(), String.t(), list(), keyword()) ::
          {:ok, execute_task()} | {:error, Error.t()}
  def query_udf(conn, %Query{} = query, package, function, args, opts \\ [])
      when is_atom(conn) and is_binary(package) and is_binary(function) and is_list(args) and
             is_list(opts) do
    case Policy.validate_query(opts) do
      {:ok, call_opts} ->
        ScanOps.query_udf(conn, query, package, function, args, call_opts)

      {:error, %NimbleOptions.ValidationError{} = e} ->
        {:error,
         Error.from_result_code(:parameter_error, message: Policy.validation_error_message(e))}
    end
  end

  @doc """
  Same as `query_udf/6` but returns the task or raises `Aerospike.Error`.
  """
  @spec query_udf!(conn(), Query.t(), String.t(), String.t(), list(), keyword()) :: execute_task()
  def query_udf!(conn, %Query{} = query, package, function, args, opts \\ [])
      when is_atom(conn) and is_binary(package) and is_binary(function) and is_list(args) and
             is_list(opts) do
    case query_udf(conn, query, package, function, args, opts) do
      {:ok, task} -> task
      {:error, %Error{} = e} -> raise e
    end
  end

  @doc """
  Builds a lazy enumerable of aggregate values from a query-wide stream UDF.

  This is distinct from `query_stream/3`: aggregate queries yield transformed
  values, not `%Aerospike.Record{}` structs. Depending on server/runtime
  behavior, the enumerable may emit one or more aggregate values.

  ## Current limitation

  The client does **not** currently run a local Lua reduction step across
  partial aggregate values. In multi-node queries, callers should therefore
  expect one or more server-emitted aggregate values and reduce them in Elixir
  if a single final value is required.
  """
  @spec query_aggregate(conn(), Query.t(), String.t(), String.t(), list(), keyword()) ::
          {:ok, Enumerable.t()} | {:error, Error.t()}
  def query_aggregate(conn, %Query{} = query, package, function, args, opts \\ [])
      when is_atom(conn) and is_binary(package) and is_binary(function) and is_list(args) and
             is_list(opts) do
    case validate_scan_query_opts(query, opts) do
      {:ok, call_opts} ->
        {:ok, ScanOps.query_aggregate(conn, query, package, function, args, call_opts)}

      {:error, %NimbleOptions.ValidationError{} = e} ->
        {:error,
         Error.from_result_code(:parameter_error, message: Policy.validation_error_message(e))}
    end
  end

  @doc """
  Same as `query_aggregate/6` but returns the enumerable or raises `Aerospike.Error`.
  """
  @spec query_aggregate!(conn(), Query.t(), String.t(), String.t(), list(), keyword()) ::
          Enumerable.t()
  def query_aggregate!(conn, %Query{} = query, package, function, args, opts \\ [])
      when is_atom(conn) and is_binary(package) and is_binary(function) and is_list(args) and
             is_list(opts) do
    case query_aggregate(conn, query, package, function, args, opts) do
      {:ok, stream} -> stream
      {:error, %Error{} = e} -> raise e
    end
  end

  @doc """
  Returns a lazy `Stream` of records from a scan or query.

  The stream yields bare `%Aerospike.Record{}` structs. If a network error
  or server error occurs mid-stream, `Aerospike.Error` is raised.

  The stream holds one pool connection per node for its entire lifetime.
  On early termination (e.g. `Enum.take/2`), the connection is closed
  rather than returned to the pool.
  In multi-node clusters, both `stream!/3` and `all/3` fan out across
  nodes concurrently by default.

  ## Options

  Scan/query policy: `:timeout`, `:pool_checkout_timeout`, `:replica`,
  `:max_concurrent_nodes` (0 = all nodes).

  ## Examples

      alias Aerospike.Scan

      Aerospike.stream!(:aero, Scan.new("test", "users"))
      |> Stream.filter(fn r -> r.bins["age"] > 21 end)
      |> Enum.take(100)

  """
  @spec stream!(conn(), Scan.t() | Query.t(), keyword()) :: Enumerable.t()
  def stream!(conn, scannable, opts \\ []) when is_atom(conn) and is_list(opts) do
    case validate_scan_query_opts(scannable, opts) do
      {:ok, call_opts} ->
        ScanOps.stream(conn, scannable, call_opts)

      {:error, %NimbleOptions.ValidationError{} = e} ->
        raise Error.from_result_code(:parameter_error,
                message: Policy.validation_error_message(e)
              )
    end
  end

  @doc """
  Eagerly collects all records from a scan or query into a list.

  Requires `max_records` on the scan/query builder. Returns
  `{:error, %Aerospike.Error{code: :max_records_required}}` if missing.

  ## Options

  Scan/query policy: `:timeout`, `:pool_checkout_timeout`, `:replica`.

  ## Examples

      alias Aerospike.Scan

      {:ok, records} = Aerospike.all(:aero, Scan.new("test", "users") |> Scan.max_records(1_000))

  """
  @spec all(conn(), Scan.t() | Query.t(), keyword()) ::
          {:ok, [Aerospike.Record.t()]} | {:error, Error.t()}
  def all(conn, scannable, opts \\ []) when is_atom(conn) and is_list(opts) do
    case validate_scan_query_opts(scannable, opts) do
      {:ok, call_opts} ->
        ScanOps.all(conn, scannable, call_opts)

      {:error, %NimbleOptions.ValidationError{} = e} ->
        {:error,
         Error.from_result_code(:parameter_error, message: Policy.validation_error_message(e))}
    end
  end

  @doc """
  Same as `all/3` but returns the list or raises `Aerospike.Error`.
  """
  @spec all!(conn(), Scan.t() | Query.t(), keyword()) :: [Aerospike.Record.t()]
  def all!(conn, scannable, opts \\ []) when is_atom(conn) and is_list(opts) do
    case all(conn, scannable, opts) do
      {:ok, records} -> records
      {:error, %Error{} = e} -> raise e
    end
  end

  @doc """
  Counts records matching a scan or query using a server-side NOBINDATA scan.

  **Cost:** This performs a full scan or query with bin payloads omitted. The
  server sends one record header per matching record, and the client counts
  them. For large datasets, prefer the Aerospike info protocol
  (`sets/<ns>/<set>`) via a raw info command for unfiltered per-set counts.

  ## Options

  Scan/query policy: `:timeout`, `:pool_checkout_timeout`, `:replica`.

  ## Examples

      alias Aerospike.Scan

      {:ok, n} = Aerospike.count(:aero, Scan.new("test", "users"))

  """
  @spec count(conn(), Scan.t() | Query.t(), keyword()) ::
          {:ok, non_neg_integer()} | {:error, Error.t()}
  def count(conn, scannable, opts \\ []) when is_atom(conn) and is_list(opts) do
    case validate_scan_query_opts(scannable, opts) do
      {:ok, call_opts} ->
        ScanOps.count(conn, scannable, call_opts)

      {:error, %NimbleOptions.ValidationError{} = e} ->
        {:error,
         Error.from_result_code(:parameter_error, message: Policy.validation_error_message(e))}
    end
  end

  @doc """
  Same as `count/3` but returns the count or raises `Aerospike.Error`.
  """
  @spec count!(conn(), Scan.t() | Query.t(), keyword()) :: non_neg_integer()
  def count!(conn, scannable, opts \\ []) when is_atom(conn) and is_list(opts) do
    case count(conn, scannable, opts) do
      {:ok, n} -> n
      {:error, %Error{} = e} -> raise e
    end
  end

  @doc """
  Returns one page of records from a scan or query with cursor-based pagination.

  Pass `cursor: page.cursor` from a previous `page/3` call to resume.

  ## Options

  * Scan/query policy: `:timeout`, `:pool_checkout_timeout`, `:replica`
  * `:cursor` — `%Aerospike.Cursor{}` or encoded cursor binary from `Cursor.encode/1`

  ## Examples

      alias Aerospike.Scan

      scan = Scan.new("test", "users") |> Scan.max_records(100)
      {:ok, page} = Aerospike.page(:aero, scan)
      {:ok, page2} = Aerospike.page(:aero, scan, cursor: page.cursor)

  """
  @spec page(conn(), Scan.t() | Query.t(), keyword()) :: {:ok, Page.t()} | {:error, Error.t()}
  def page(conn, scannable, opts \\ []) when is_atom(conn) and is_list(opts) do
    {cursor, opts2} = Keyword.pop(opts, :cursor)

    case validate_scan_query_opts(scannable, opts2) do
      {:ok, call_opts} ->
        opts3 = if cursor != nil, do: Keyword.put(call_opts, :cursor, cursor), else: call_opts
        ScanOps.page(conn, scannable, opts3)

      {:error, %NimbleOptions.ValidationError{} = e} ->
        {:error,
         Error.from_result_code(:parameter_error, message: Policy.validation_error_message(e))}
    end
  end

  @doc """
  Same as `page/3` but returns `%Aerospike.Page{}` or raises `Aerospike.Error`.
  """
  @spec page!(conn(), Scan.t() | Query.t(), keyword()) :: Page.t()
  def page!(conn, scannable, opts \\ []) when is_atom(conn) and is_list(opts) do
    case page(conn, scannable, opts) do
      {:ok, p} -> p
      {:error, %Error{} = e} -> raise e
    end
  end

  @doc """
  Returns a lazy `Stream` of scan records from one named node.

  Callers obtain `node_name` from `node_names/1` or from the `:name` field in
  `nodes/1`. Missing or stale node names return a normal `%Aerospike.Error{}`
  rather than silently broadening the scan to the whole cluster.
  """
  @spec scan_stream_node!(conn(), node_name(), Scan.t(), keyword()) :: Enumerable.t()
  def scan_stream_node!(conn, node_name, %Scan{} = scan, opts \\ [])
      when is_atom(conn) and is_binary(node_name) and is_list(opts) do
    case validate_scan_query_opts(scan, opts) do
      {:ok, call_opts} ->
        ScanOps.stream_node(conn, node_name, scan, call_opts)

      {:error, %NimbleOptions.ValidationError{} = e} ->
        raise Error.from_result_code(:parameter_error,
                message: Policy.validation_error_message(e)
              )
    end
  end

  @doc """
  Eagerly collects scan records from one named node.
  """
  @spec scan_all_node(conn(), node_name(), Scan.t(), keyword()) ::
          {:ok, [Aerospike.Record.t()]} | {:error, Error.t()}
  def scan_all_node(conn, node_name, %Scan{} = scan, opts \\ [])
      when is_atom(conn) and is_binary(node_name) and is_list(opts) do
    case validate_scan_query_opts(scan, opts) do
      {:ok, call_opts} ->
        ScanOps.all_node(conn, node_name, scan, call_opts)

      {:error, %NimbleOptions.ValidationError{} = e} ->
        {:error,
         Error.from_result_code(:parameter_error, message: Policy.validation_error_message(e))}
    end
  end

  @doc """
  Same as `scan_all_node/4` but returns the list or raises `Aerospike.Error`.
  """
  @spec scan_all_node!(conn(), node_name(), Scan.t(), keyword()) :: [Aerospike.Record.t()]
  def scan_all_node!(conn, node_name, %Scan{} = scan, opts \\ [])
      when is_atom(conn) and is_binary(node_name) and is_list(opts) do
    case scan_all_node(conn, node_name, scan, opts) do
      {:ok, records} -> records
      {:error, %Error{} = e} -> raise e
    end
  end

  @doc """
  Counts scan matches on one named node.
  """
  @spec scan_count_node(conn(), node_name(), Scan.t(), keyword()) ::
          {:ok, non_neg_integer()} | {:error, Error.t()}
  def scan_count_node(conn, node_name, %Scan{} = scan, opts \\ [])
      when is_atom(conn) and is_binary(node_name) and is_list(opts) do
    case validate_scan_query_opts(scan, opts) do
      {:ok, call_opts} ->
        ScanOps.count_node(conn, node_name, scan, call_opts)

      {:error, %NimbleOptions.ValidationError{} = e} ->
        {:error,
         Error.from_result_code(:parameter_error, message: Policy.validation_error_message(e))}
    end
  end

  @doc """
  Same as `scan_count_node/4` but returns the count or raises `Aerospike.Error`.
  """
  @spec scan_count_node!(conn(), node_name(), Scan.t(), keyword()) :: non_neg_integer()
  def scan_count_node!(conn, node_name, %Scan{} = scan, opts \\ [])
      when is_atom(conn) and is_binary(node_name) and is_list(opts) do
    case scan_count_node(conn, node_name, scan, opts) do
      {:ok, n} -> n
      {:error, %Error{} = e} -> raise e
    end
  end

  @doc """
  Returns one page of scan results from one named node.
  """
  @spec scan_page_node(conn(), node_name(), Scan.t(), keyword()) ::
          {:ok, Page.t()} | {:error, Error.t()}
  def scan_page_node(conn, node_name, %Scan{} = scan, opts \\ [])
      when is_atom(conn) and is_binary(node_name) and is_list(opts) do
    {cursor, opts2} = Keyword.pop(opts, :cursor)

    case validate_scan_query_opts(scan, opts2) do
      {:ok, call_opts} ->
        opts3 = if cursor != nil, do: Keyword.put(call_opts, :cursor, cursor), else: call_opts
        ScanOps.page_node(conn, node_name, scan, opts3)

      {:error, %NimbleOptions.ValidationError{} = e} ->
        {:error,
         Error.from_result_code(:parameter_error, message: Policy.validation_error_message(e))}
    end
  end

  @doc """
  Same as `scan_page_node/4` but returns `%Aerospike.Page{}` or raises `Aerospike.Error`.
  """
  @spec scan_page_node!(conn(), node_name(), Scan.t(), keyword()) :: Page.t()
  def scan_page_node!(conn, node_name, %Scan{} = scan, opts \\ [])
      when is_atom(conn) and is_binary(node_name) and is_list(opts) do
    case scan_page_node(conn, node_name, scan, opts) do
      {:ok, page} -> page
      {:error, %Error{} = e} -> raise e
    end
  end

  @doc """
  Sends a raw info command to a random cluster node and returns the response string.

  ## Options

  `:timeout`, `:pool_checkout_timeout`

  ## Example

      {:ok, "test"} = Aerospike.info(:aero, "namespaces")

  """
  @spec info(conn(), String.t(), keyword()) :: {:ok, String.t()} | {:error, Error.t()}
  def info(conn, command, opts \\ [])
      when is_atom(conn) and is_binary(command) and is_list(opts) do
    case Policy.validate_info(opts) do
      {:ok, call_opts} ->
        Admin.info(conn, command, call_opts)

      {:error, %NimbleOptions.ValidationError{} = e} ->
        {:error,
         Error.from_result_code(:parameter_error, message: Policy.validation_error_message(e))}
    end
  end

  @doc """
  Sends a raw info command to the named cluster node and returns the response string.

  ## Options

  `:timeout`, `:pool_checkout_timeout`

  ## Example

      {:ok, [%{name: name}]} = Aerospike.nodes(:aero)
      {:ok, response} = Aerospike.info_node(:aero, name, "statistics")

  """
  @spec info_node(conn(), String.t(), String.t(), keyword()) ::
          {:ok, String.t()} | {:error, Error.t()}
  def info_node(conn, node_name, command, opts \\ [])
      when is_atom(conn) and is_binary(node_name) and is_binary(command) and is_list(opts) do
    case Policy.validate_info(opts) do
      {:ok, call_opts} ->
        Admin.info_node(conn, node_name, command, call_opts)

      {:error, %NimbleOptions.ValidationError{} = e} ->
        {:error,
         Error.from_result_code(:parameter_error, message: Policy.validation_error_message(e))}
    end
  end

  @doc """
  Returns the list of cluster nodes with their name, host, and port.

  Each element matches `t:Aerospike.node_info/0`.

  ## Example

      {:ok, [%{name: "BB9...", host: "127.0.0.1", port: 3000}]} = Aerospike.nodes(:aero)

  """
  @spec nodes(conn()) :: {:ok, [node_info()]} | {:error, Error.t()}
  def nodes(conn) when is_atom(conn), do: Admin.nodes(conn)

  @doc """
  Returns the list of cluster node name strings.

  ## Example

      {:ok, ["BB9..."]} = Aerospike.node_names(:aero)

  """
  @spec node_names(conn()) :: {:ok, [String.t()]} | {:error, Error.t()}
  def node_names(conn) when is_atom(conn), do: Admin.node_names(conn)

  @doc """
  Creates a password-authenticated security user.

  `password` is accepted as cleartext at the facade boundary and hashed before
  the wire command is sent.

  ## Options

  * `:timeout` — socket timeout in milliseconds.
  * `:pool_checkout_timeout` — pool checkout timeout in milliseconds.

  """
  @spec create_user(conn(), String.t(), String.t(), [String.t()], keyword()) ::
          :ok | {:error, Error.t()}
  def create_user(conn, user_name, password, roles, opts \\ [])
      when is_atom(conn) and is_binary(user_name) and is_binary(password) and is_list(roles) and
             is_list(opts) do
    with {:ok, call_opts} <- Policy.validate_security_admin(opts),
         {:ok, role_names} <- validate_role_names(roles) do
      Admin.create_user(conn, user_name, password, role_names, call_opts)
    else
      {:error, %Error{} = e} ->
        {:error, e}

      {:error, %NimbleOptions.ValidationError{} = e} ->
        {:error,
         Error.from_result_code(:parameter_error, message: Policy.validation_error_message(e))}
    end
  end

  @doc """
  Creates a PKI-authenticated security user.

  Supported by Aerospike Enterprise on server versions that support PKI users.

  ## Options

  * `:timeout` — socket timeout in milliseconds.
  * `:pool_checkout_timeout` — pool checkout timeout in milliseconds.

  """
  @spec create_pki_user(conn(), String.t(), [String.t()], keyword()) :: :ok | {:error, Error.t()}
  def create_pki_user(conn, user_name, roles, opts \\ [])
      when is_atom(conn) and is_binary(user_name) and is_list(roles) and is_list(opts) do
    with {:ok, call_opts} <- Policy.validate_security_admin(opts),
         {:ok, role_names} <- validate_role_names(roles) do
      Admin.create_pki_user(conn, user_name, role_names, call_opts)
    else
      {:error, %Error{} = e} ->
        {:error, e}

      {:error, %NimbleOptions.ValidationError{} = e} ->
        {:error,
         Error.from_result_code(:parameter_error, message: Policy.validation_error_message(e))}
    end
  end

  @doc """
  Drops a security user.
  """
  @spec drop_user(conn(), String.t(), keyword()) :: :ok | {:error, Error.t()}
  def drop_user(conn, user_name, opts \\ [])
      when is_atom(conn) and is_binary(user_name) and is_list(opts) do
    case Policy.validate_security_admin(opts) do
      {:ok, call_opts} ->
        Admin.drop_user(conn, user_name, call_opts)

      {:error, %NimbleOptions.ValidationError{} = e} ->
        {:error,
         Error.from_result_code(:parameter_error, message: Policy.validation_error_message(e))}
    end
  end

  @doc """
  Changes a security user's password.

  When `user_name` matches the authenticated connection user, the client uses
  the server change-password command and rotates the running client's in-memory
  credential source for future reconnects. Otherwise it uses the user-admin
  password-set path.

  Restarted clients still authenticate with the credentials supplied at startup
  in `:auth_opts`; the library does not persist rotated passwords.
  """
  @spec change_password(conn(), String.t(), String.t(), keyword()) :: :ok | {:error, Error.t()}
  def change_password(conn, user_name, password, opts \\ [])
      when is_atom(conn) and is_binary(user_name) and is_binary(password) and is_list(opts) do
    case Policy.validate_security_admin(opts) do
      {:ok, call_opts} ->
        Admin.change_password(conn, user_name, password, call_opts)

      {:error, %NimbleOptions.ValidationError{} = e} ->
        {:error,
         Error.from_result_code(:parameter_error, message: Policy.validation_error_message(e))}
    end
  end

  @doc """
  Grants roles to a security user.
  """
  @spec grant_roles(conn(), String.t(), [String.t()], keyword()) :: :ok | {:error, Error.t()}
  def grant_roles(conn, user_name, roles, opts \\ [])
      when is_atom(conn) and is_binary(user_name) and is_list(roles) and is_list(opts) do
    with {:ok, call_opts} <- Policy.validate_security_admin(opts),
         {:ok, role_names} <- validate_role_names(roles) do
      Admin.grant_roles(conn, user_name, role_names, call_opts)
    else
      {:error, %Error{} = e} ->
        {:error, e}

      {:error, %NimbleOptions.ValidationError{} = e} ->
        {:error,
         Error.from_result_code(:parameter_error, message: Policy.validation_error_message(e))}
    end
  end

  @doc """
  Revokes roles from a security user.
  """
  @spec revoke_roles(conn(), String.t(), [String.t()], keyword()) :: :ok | {:error, Error.t()}
  def revoke_roles(conn, user_name, roles, opts \\ [])
      when is_atom(conn) and is_binary(user_name) and is_list(roles) and is_list(opts) do
    with {:ok, call_opts} <- Policy.validate_security_admin(opts),
         {:ok, role_names} <- validate_role_names(roles) do
      Admin.revoke_roles(conn, user_name, role_names, call_opts)
    else
      {:error, %Error{} = e} ->
        {:error, e}

      {:error, %NimbleOptions.ValidationError{} = e} ->
        {:error,
         Error.from_result_code(:parameter_error, message: Policy.validation_error_message(e))}
    end
  end

  @doc """
  Queries metadata for one security user.
  """
  @spec query_user(conn(), String.t(), keyword()) ::
          {:ok, user_info() | nil} | {:error, Error.t()}
  def query_user(conn, user_name, opts \\ [])
      when is_atom(conn) and is_binary(user_name) and is_list(opts) do
    case Policy.validate_security_admin(opts) do
      {:ok, call_opts} ->
        Admin.query_user(conn, user_name, call_opts)

      {:error, %NimbleOptions.ValidationError{} = e} ->
        {:error,
         Error.from_result_code(:parameter_error, message: Policy.validation_error_message(e))}
    end
  end

  @doc """
  Queries metadata for all security users.
  """
  @spec query_users(conn(), keyword()) :: {:ok, [user_info()]} | {:error, Error.t()}
  def query_users(conn, opts \\ []) when is_atom(conn) and is_list(opts) do
    case Policy.validate_security_admin(opts) do
      {:ok, call_opts} ->
        Admin.query_users(conn, call_opts)

      {:error, %NimbleOptions.ValidationError{} = e} ->
        {:error,
         Error.from_result_code(:parameter_error, message: Policy.validation_error_message(e))}
    end
  end

  @doc """
  Creates a user-defined security role.

  ## Options

  * `:whitelist` — list of allowed client IPs or CIDR ranges.
  * `:read_quota` — max reads per second. `0` means unlimited.
  * `:write_quota` — max writes per second. `0` means unlimited.
  * `:timeout` — socket timeout in milliseconds.
  * `:pool_checkout_timeout` — pool checkout timeout in milliseconds.

  """
  @spec create_role(conn(), String.t(), [privilege()], keyword()) :: :ok | {:error, Error.t()}
  def create_role(conn, role_name, privileges, opts \\ [])
      when is_atom(conn) and is_binary(role_name) and is_list(privileges) and is_list(opts) do
    with {:ok, call_opts} <- Policy.validate_role_create(opts),
         {:ok, validated_privileges} <- validate_privileges(privileges) do
      Admin.create_role(conn, role_name, validated_privileges, call_opts)
    else
      {:error, %Error{} = e} ->
        {:error, e}

      {:error, %NimbleOptions.ValidationError{} = e} ->
        {:error,
         Error.from_result_code(:parameter_error, message: Policy.validation_error_message(e))}
    end
  end

  @doc """
  Drops a user-defined security role.
  """
  @spec drop_role(conn(), String.t(), keyword()) :: :ok | {:error, Error.t()}
  def drop_role(conn, role_name, opts \\ [])
      when is_atom(conn) and is_binary(role_name) and is_list(opts) do
    case Policy.validate_security_admin(opts) do
      {:ok, call_opts} ->
        Admin.drop_role(conn, role_name, call_opts)

      {:error, %NimbleOptions.ValidationError{} = e} ->
        {:error,
         Error.from_result_code(:parameter_error, message: Policy.validation_error_message(e))}
    end
  end

  @doc """
  Grants privileges to a user-defined security role.
  """
  @spec grant_privileges(conn(), String.t(), [privilege()], keyword()) ::
          :ok | {:error, Error.t()}
  def grant_privileges(conn, role_name, privileges, opts \\ [])
      when is_atom(conn) and is_binary(role_name) and is_list(privileges) and is_list(opts) do
    with {:ok, call_opts} <- Policy.validate_security_admin(opts),
         {:ok, validated_privileges} <- validate_privileges(privileges) do
      Admin.grant_privileges(conn, role_name, validated_privileges, call_opts)
    else
      {:error, %Error{} = e} ->
        {:error, e}

      {:error, %NimbleOptions.ValidationError{} = e} ->
        {:error,
         Error.from_result_code(:parameter_error, message: Policy.validation_error_message(e))}
    end
  end

  @doc """
  Revokes privileges from a user-defined security role.
  """
  @spec revoke_privileges(conn(), String.t(), [privilege()], keyword()) ::
          :ok | {:error, Error.t()}
  def revoke_privileges(conn, role_name, privileges, opts \\ [])
      when is_atom(conn) and is_binary(role_name) and is_list(privileges) and is_list(opts) do
    with {:ok, call_opts} <- Policy.validate_security_admin(opts),
         {:ok, validated_privileges} <- validate_privileges(privileges) do
      Admin.revoke_privileges(conn, role_name, validated_privileges, call_opts)
    else
      {:error, %Error{} = e} ->
        {:error, e}

      {:error, %NimbleOptions.ValidationError{} = e} ->
        {:error,
         Error.from_result_code(:parameter_error, message: Policy.validation_error_message(e))}
    end
  end

  @doc """
  Sets or clears the whitelist for a security role.
  """
  @spec set_whitelist(conn(), String.t(), Role.whitelist(), keyword()) ::
          :ok | {:error, Error.t()}
  def set_whitelist(conn, role_name, whitelist, opts \\ [])
      when is_atom(conn) and is_binary(role_name) and is_list(whitelist) and is_list(opts) do
    with {:ok, call_opts} <- Policy.validate_security_admin(opts),
         {:ok, validated_whitelist} <- validate_whitelist(whitelist) do
      Admin.set_whitelist(conn, role_name, validated_whitelist, call_opts)
    else
      {:error, %Error{} = e} ->
        {:error, e}

      {:error, %NimbleOptions.ValidationError{} = e} ->
        {:error,
         Error.from_result_code(:parameter_error, message: Policy.validation_error_message(e))}
    end
  end

  @doc """
  Sets read and write quotas for a security role.

  `0` removes the corresponding limit.
  """
  @spec set_quotas(conn(), String.t(), Role.quota(), Role.quota(), keyword()) ::
          :ok | {:error, Error.t()}
  def set_quotas(conn, role_name, read_quota, write_quota, opts \\ [])
      when is_atom(conn) and is_binary(role_name) and is_integer(read_quota) and
             read_quota >= 0 and is_integer(write_quota) and write_quota >= 0 and
             is_list(opts) do
    case Policy.validate_security_admin(opts) do
      {:ok, call_opts} ->
        Admin.set_quotas(conn, role_name, read_quota, write_quota, call_opts)

      {:error, %NimbleOptions.ValidationError{} = e} ->
        {:error,
         Error.from_result_code(:parameter_error, message: Policy.validation_error_message(e))}
    end
  end

  @doc """
  Queries metadata for one security role.
  """
  @spec query_role(conn(), String.t(), keyword()) ::
          {:ok, role_info() | nil} | {:error, Error.t()}
  def query_role(conn, role_name, opts \\ [])
      when is_atom(conn) and is_binary(role_name) and is_list(opts) do
    case Policy.validate_security_admin(opts) do
      {:ok, call_opts} ->
        Admin.query_role(conn, role_name, call_opts)

      {:error, %NimbleOptions.ValidationError{} = e} ->
        {:error,
         Error.from_result_code(:parameter_error, message: Policy.validation_error_message(e))}
    end
  end

  @doc """
  Queries metadata for all security roles.
  """
  @spec query_roles(conn(), keyword()) :: {:ok, [role_info()]} | {:error, Error.t()}
  def query_roles(conn, opts \\ []) when is_atom(conn) and is_list(opts) do
    case Policy.validate_security_admin(opts) do
      {:ok, call_opts} ->
        Admin.query_roles(conn, call_opts)

      {:error, %NimbleOptions.ValidationError{} = e} ->
        {:error,
         Error.from_result_code(:parameter_error, message: Policy.validation_error_message(e))}
    end
  end

  @doc """
  Truncates all records in `namespace`, optionally only those written before `before:`.

  ## Options

  * `:before` — `%DateTime{}` — truncate only records written before this timestamp.
  * `:pool_checkout_timeout` — pool checkout timeout in ms.

  ## Example

      :ok = Aerospike.truncate(:aero, "test")
      :ok = Aerospike.truncate(:aero, "test", before: DateTime.utc_now())

  """
  @spec truncate(conn(), String.t(), keyword()) :: :ok | {:error, Error.t()}
  def truncate(conn, namespace, opts \\ [])

  def truncate(conn, namespace, set)
      when is_atom(conn) and is_binary(namespace) and is_binary(set) do
    truncate(conn, namespace, set, [])
  end

  def truncate(conn, namespace, opts)
      when is_atom(conn) and is_binary(namespace) and is_list(opts) do
    case Policy.validate_info(Keyword.delete(opts, :before)) do
      {:ok, call_opts} ->
        Admin.truncate(conn, namespace, Keyword.merge(call_opts, Keyword.take(opts, [:before])))

      {:error, %NimbleOptions.ValidationError{} = e} ->
        {:error,
         Error.from_result_code(:parameter_error, message: Policy.validation_error_message(e))}
    end
  end

  @doc """
  Truncates all records in `namespace` and `set`, optionally those written before `before:`.

  ## Options

  * `:before` — `%DateTime{}` — truncate only records written before this timestamp.
  * `:pool_checkout_timeout` — pool checkout timeout in ms.

  ## Example

      :ok = Aerospike.truncate(:aero, "test", "users")
      :ok = Aerospike.truncate(:aero, "test", "users", before: DateTime.utc_now())

  """
  @spec truncate(conn(), String.t(), String.t(), keyword()) :: :ok | {:error, Error.t()}
  def truncate(conn, namespace, set, opts)
      when is_atom(conn) and is_binary(namespace) and is_binary(set) and is_list(opts) do
    case Policy.validate_info(Keyword.delete(opts, :before)) do
      {:ok, call_opts} ->
        Admin.truncate(
          conn,
          namespace,
          set,
          Keyword.merge(call_opts, Keyword.take(opts, [:before]))
        )

      {:error, %NimbleOptions.ValidationError{} = e} ->
        {:error,
         Error.from_result_code(:parameter_error, message: Policy.validation_error_message(e))}
    end
  end

  @doc """
  Creates a secondary index on `set` in `namespace`.

  The server builds the index in the background and returns an `IndexTask` that
  you can poll with `IndexTask.status/1` or block on with `IndexTask.wait/2`.

  ## Options

  * `:bin` — required string; the bin name to index.
  * `:name` — required string; the index name.
  * `:type` — required atom; `:numeric`, `:string`, or `:geo2dsphere`.
  * `:collection` — optional atom; `:list`, `:mapkeys`, or `:mapvalues` for CDT bins.
  * `:ctx` — optional `t:Aerospike.Ctx.step/0` list for nested CDT indexes.
  * `:pool_checkout_timeout` — pool checkout timeout in ms.

  ## Example

      {:ok, task} = Aerospike.create_index(:aero, "test", "demo",
        bin: "age", name: "age_idx", type: :numeric
      )
      :ok = Aerospike.IndexTask.wait(task, timeout: 30_000)

  """
  @spec create_index(conn(), String.t(), String.t(), keyword()) ::
          {:ok, IndexTask.t()} | {:error, Error.t()}
  def create_index(conn, namespace, set, opts)
      when is_atom(conn) and is_binary(namespace) and is_binary(set) and is_list(opts) do
    case Policy.validate_index_create(opts) do
      {:ok, call_opts} ->
        Admin.create_index(conn, namespace, set, call_opts)

      {:error, %NimbleOptions.ValidationError{} = e} ->
        {:error,
         Error.from_result_code(:parameter_error, message: Policy.validation_error_message(e))}
    end
  end

  @doc """
  Creates a secondary index backed by a server expression instead of a bin name.

  This is the explicit public entry point for expression-backed secondary indexes.
  Expression-backed indexes are distinct from bin-backed indexes because they omit
  `:bin` entirely and use `expression` as the source definition.

  ## Options

  * `:name` — required string; the index name.
  * `:type` — required atom; `:numeric`, `:string`, or `:geo2dsphere`.
  * `:collection` — optional atom; `:list`, `:mapkeys`, or `:mapvalues` when the
    expression evaluates to a collection.
  * `:pool_checkout_timeout` — pool checkout timeout in ms.

  Expression-backed secondary indexes require Aerospike server `8.1.0` or newer.

  ## Example

      expression =
        Aerospike.Exp.int_bin("age")
        |> Aerospike.Exp.gt(Aerospike.Exp.val(17))

      {:ok, task} =
        Aerospike.create_expression_index(:aero, "test", "users", expression,
          name: "adult_users_idx",
          type: :numeric
        )

      :ok = Aerospike.IndexTask.wait(task, timeout: 30_000)

  """
  @spec create_expression_index(conn(), String.t(), String.t(), Exp.t(), keyword()) ::
          {:ok, IndexTask.t()} | {:error, Error.t()}
  def create_expression_index(conn, namespace, set, %Exp{} = expression, opts)
      when is_atom(conn) and is_binary(namespace) and is_binary(set) and is_list(opts) do
    case Policy.validate_expression_index_create(expression, opts) do
      {:ok, call_opts} ->
        Admin.create_expression_index(conn, namespace, set, expression, call_opts)

      {:error, %NimbleOptions.ValidationError{} = e} ->
        {:error,
         Error.from_result_code(:parameter_error, message: Policy.validation_error_message(e))}
    end
  end

  @doc """
  Drops a secondary index by name.

  Returns `:ok` if the index was dropped or did not exist.

  ## Example

      :ok = Aerospike.drop_index(:aero, "test", "age_idx")

  """
  @spec drop_index(conn(), String.t(), String.t()) :: :ok | {:error, Error.t()}
  def drop_index(conn, namespace, index_name)
      when is_atom(conn) and is_binary(namespace) and is_binary(index_name) do
    Admin.drop_index(conn, namespace, index_name, [])
  end

  @doc """
  Registers a UDF package on the cluster.

  `path_or_content` can be either a filesystem path to a `.lua` file or the
  raw Lua source as a string. `server_name` is the package name used on the
  server (typically the filename, e.g. `"my_module.lua"`).

  Returns an `RegisterTask` that you can poll with `RegisterTask.status/1` or
  block on with `RegisterTask.wait/2`.

  ## Example

      {:ok, task} = Aerospike.register_udf(:aero, "/path/to/my_module.lua", "my_module.lua")
      :ok = Aerospike.RegisterTask.wait(task, timeout: 10_000)

  """
  @spec register_udf(conn(), String.t(), String.t()) ::
          {:ok, RegisterTask.t()} | {:error, Error.t()}
  def register_udf(conn, path_or_content, server_name)
      when is_atom(conn) and is_binary(path_or_content) and is_binary(server_name) do
    Admin.register_udf(conn, path_or_content, server_name, [])
  end

  @doc """
  Registers a UDF package on the cluster with options.

  See `register_udf/3` for details.

  ## Options

  * `:pool_checkout_timeout` — pool checkout timeout in ms.

  """
  @spec register_udf(conn(), String.t(), String.t(), keyword()) ::
          {:ok, RegisterTask.t()} | {:error, Error.t()}
  def register_udf(conn, path_or_content, server_name, opts)
      when is_atom(conn) and is_binary(path_or_content) and is_binary(server_name) and
             is_list(opts) do
    Admin.register_udf(conn, path_or_content, server_name, opts)
  end

  @doc """
  Removes a UDF package from the cluster.

  Returns `:ok` whether or not the package was registered.

  ## Example

      :ok = Aerospike.remove_udf(:aero, "my_module.lua")

  """
  @spec remove_udf(conn(), String.t()) :: :ok | {:error, Error.t()}
  def remove_udf(conn, udf_name)
      when is_atom(conn) and is_binary(udf_name) do
    Admin.remove_udf(conn, udf_name, [])
  end

  @doc """
  Removes a UDF package from the cluster with options.

  See `remove_udf/2` for details.

  ## Options

  * `:pool_checkout_timeout` — pool checkout timeout in ms.

  """
  @spec remove_udf(conn(), String.t(), keyword()) :: :ok | {:error, Error.t()}
  def remove_udf(conn, udf_name, opts)
      when is_atom(conn) and is_binary(udf_name) and is_list(opts) do
    Admin.remove_udf(conn, udf_name, opts)
  end

  @doc """
  Executes a UDF (User Defined Function) on a single record.

  Accepts tuple keys; see `put/4`.

  `package` is the Lua module name as registered on the server (without the `.lua`
  extension). `function` is the Lua function name. `args` is the list of arguments
  passed to the function.

  Returns `{:ok, value}` where `value` is the Lua function's return value, or
  `{:error, %Error{code: :udf_bad_response}}` on UDF runtime error.

  ## Example

      {:ok, task} = Aerospike.register_udf(:aero, "/path/my.lua", "my.lua")
      :ok = Aerospike.RegisterTask.wait(task)

      key = Aerospike.key("test", "demo", "k1")
      :ok = Aerospike.put(:aero, key, %{"n" => 42})
      {:ok, result} = Aerospike.apply_udf(:aero, key, "my", "double_n", [])

  """
  @spec apply_udf(conn(), Key.key_input(), String.t(), String.t(), list()) ::
          {:ok, term()} | {:error, Error.t()}
  def apply_udf(conn, key, package, function, args)
      when is_atom(conn) and is_binary(package) and is_binary(function) and is_list(args) do
    with {:ok, key} <- coerce_key(key) do
      CRUD.apply_udf(conn, key, package, function, args, [])
    end
  end

  @doc """
  Executes a UDF on a single record with options.

  See `apply_udf/5` for details.

  ## Options

  * `:timeout` — socket timeout in milliseconds.
  * `:filter` — server-side expression filter (`%Aerospike.Exp{}`).
  * `:pool_checkout_timeout` — pool checkout timeout in milliseconds.
  * `:replica` — replica routing: `:master`, `:sequence`, or `:any`.

  """
  @spec apply_udf(conn(), Key.key_input(), String.t(), String.t(), list(), keyword()) ::
          {:ok, term()} | {:error, Error.t()}
  def apply_udf(conn, key, package, function, args, opts)
      when is_atom(conn) and is_binary(package) and is_binary(function) and is_list(args) and
             is_list(opts) do
    with {:ok, key} <- coerce_key(key),
         {:ok, validated} <- Policy.validate_udf(opts) do
      CRUD.apply_udf(conn, key, package, function, args, validated)
    else
      {:error, %Error{} = e} ->
        {:error, e}

      {:error, e} ->
        {:error,
         Error.from_result_code(:parameter_error, message: Policy.validation_error_message(e))}
    end
  end

  @doc """
  Commits a transaction.

  Runs the multi-phase commit protocol: verifies all tracked reads, marks the
  server-side monitor record for roll-forward, and finalizes each write.

  Returns `{:ok, :committed}` on success, `{:ok, :already_committed}` if the
  transaction was already committed, or `{:error, %Aerospike.Error{}}` on failure.

  Most callers should use `transaction/2` or `transaction/3`, which handle
  initialization, commit, and abort automatically. Use `commit/2` directly only
  when managing the transaction lifecycle manually (without the `transaction`
  wrapper).

  ## Example

      txn = Txn.new()
      TxnOps.init_tracking(:conn, txn)
      Aerospike.put(:conn, key1, %{"x" => 1}, txn: txn)
      Aerospike.put(:conn, key2, %{"x" => 2}, txn: txn)
      {:ok, :committed} = Aerospike.commit(:conn, txn)

  """
  @spec commit(conn(), Txn.t()) :: {:ok, :committed | :already_committed} | {:error, Error.t()}
  def commit(conn, %Txn{} = txn) when is_atom(conn) do
    TxnRoll.commit(conn, txn, [])
  end

  @doc """
  Aborts a transaction, rolling back all writes.

  Returns `{:ok, :aborted}` on success, `{:ok, :already_aborted}` if the
  transaction was already aborted, or `{:error, %Aerospike.Error{}}` if the
  transaction was already committed.

  Roll-back writes are best-effort: if some fail (e.g., during a network
  partition), the server releases remaining locks when the MRT timeout expires.

  Most callers should use `transaction/2` or `transaction/3`, which abort
  automatically on failure. Use `abort/2` directly only when managing the
  transaction lifecycle manually (without the `transaction` wrapper).

  ## Example

      txn = Txn.new()
      TxnOps.init_tracking(:conn, txn)
      Aerospike.put(:conn, key, %{"x" => 1}, txn: txn)
      {:ok, :aborted} = Aerospike.abort(:conn, txn)

  """
  @spec abort(conn(), Txn.t()) :: {:ok, :aborted | :already_aborted} | {:error, Error.t()}
  def abort(conn, %Txn{} = txn) when is_atom(conn) do
    TxnRoll.abort(conn, txn, [])
  end

  @doc """
  Returns the current state of a transaction.

  Primarily useful **inside** a `transaction/2` or `transaction/3` callback to
  check whether the transaction is still open. After a successful commit or
  abort, the transaction's ETS tracking is cleaned up, so this function returns
  `{:error, %Aerospike.Error{}}` rather than `{:ok, :committed}` or
  `{:ok, :aborted}`.

  ## States

  - `:open` — transaction is active and accepting operations
  - `:verified` — verify phase completed (intermediate state during commit)

  ## Example

      Aerospike.transaction(:conn, fn txn ->
        {:ok, :open} = Aerospike.txn_status(:conn, txn)
        Aerospike.put(:conn, key, %{"x" => 1}, txn: txn)
      end)

  """
  @spec txn_status(conn(), Txn.t()) ::
          {:ok, :open | :verified | :committed | :aborted} | {:error, Error.t()}
  def txn_status(conn, %Txn{} = txn) when is_atom(conn) do
    TxnRoll.txn_status(conn, txn)
  end

  @doc """
  Runs a function within a new transaction and commits or aborts automatically.

  Creates a new transaction, calls `fun.(txn)` with the transaction handle, then:

  - Commits on success (non-exception return).
  - Aborts and returns `{:error, e}` if `fun` raises `%Aerospike.Error{}`.
  - Aborts and re-raises if `fun` raises any other exception, throws, or exits.

  Abort runs on **all** failure paths (not just `%Aerospike.Error{}`), so
  server-side write locks are released immediately instead of waiting for the
  MRT timeout to expire.

  Returns `{:ok, fun_result}` on successful commit.

  Do not call `commit/2` or `abort/2` directly inside `fun` — the wrapper
  manages both automatically. If you call `abort/2` inside the callback and
  then return normally, the auto-commit will fail because the transaction's
  tracking state has already been cleaned up. Use `commit/2` and `abort/2`
  only when managing the transaction lifecycle manually (without this wrapper).

  ## Example

      {:ok, _} =
        Aerospike.transaction(:conn, fn txn ->
          Aerospike.put(:conn, key1, %{"x" => 1}, txn: txn)
          Aerospike.put(:conn, key2, %{"x" => 2}, txn: txn)
        end)

  """
  @spec transaction(conn(), (Txn.t() -> term())) :: {:ok, term()} | {:error, Error.t()}
  def transaction(conn, fun) when is_atom(conn) and is_function(fun, 1) do
    TxnRoll.transaction(conn, [], fun)
  end

  @doc """
  Runs a function within a transaction using a provided handle or options, committing or aborting automatically.

  When `txn_or_opts` is a `%Aerospike.Txn{}`, the existing transaction handle is used and
  tracking is initialized fresh. When it is a keyword list, a new transaction is created
  with those options (e.g., `timeout: 5_000`).

  See `transaction/2` for commit/abort behavior.

  ## Example

      {:ok, _} =
        Aerospike.transaction(:conn, [timeout: 5_000], fn txn ->
          Aerospike.put(:conn, key, %{"x" => 1}, txn: txn)
        end)

  """
  @spec transaction(conn(), Txn.t() | keyword(), (Txn.t() -> term())) ::
          {:ok, term()} | {:error, Error.t()}
  def transaction(conn, txn_or_opts, fun)
      when is_atom(conn) and is_function(fun, 1) do
    TxnRoll.transaction(conn, txn_or_opts, fun)
  end

  defp coerce_key(key) do
    {:ok, Key.coerce!(key)}
  rescue
    e in ArgumentError ->
      {:error, Error.from_result_code(:parameter_error, message: e.message)}
  end

  defp coerce_keys(keys) do
    {:ok, Enum.map(keys, &Key.coerce!/1)}
  rescue
    e in ArgumentError ->
      {:error, Error.from_result_code(:parameter_error, message: e.message)}
  end

  defp validate_scan_query_opts(%Scan{} = _scannable, opts) when is_list(opts),
    do: Policy.validate_scan(opts)

  defp validate_scan_query_opts(%Query{} = _scannable, opts) when is_list(opts),
    do: Policy.validate_query(opts)

  defp validate_batch_get_header_opts(opts) when is_list(opts) do
    cond do
      Keyword.has_key?(opts, :bins) ->
        {:error,
         Error.from_result_code(:parameter_error,
           message: "batch_get_header/3 does not accept :bins; it always returns headers only"
         )}

      Keyword.has_key?(opts, :header_only) ->
        {:error,
         Error.from_result_code(:parameter_error,
           message: "batch_get_header/3 always uses header_only: true; omit :header_only"
         )}

      true ->
        :ok
    end
  end

  defp validate_batch_get_operate_ops([]) do
    {:error,
     Error.from_result_code(:parameter_error, message: "batch operate operations cannot be empty")}
  end

  defp validate_batch_get_operate_ops(ops) when is_list(ops) do
    flags = OperateFlags.scan_ops(ops)

    if flags.has_write? do
      {:error,
       Error.from_result_code(:parameter_error,
         message: "batch_get_operate/4 accepts only read-only operations"
       )}
    else
      :ok
    end
  end

  defp batch_operate_ops(keys, ops) do
    Enum.map(keys, &Batch.operate(&1, ops))
  end

  defp normalize_batch_get_operate_results(results) when is_list(results) do
    Enum.reduce_while(results, {:ok, []}, fn result, {:ok, acc} ->
      case normalize_batch_get_operate_result(result) do
        {:ok, record} -> {:cont, {:ok, [record | acc]}}
        {:error, _} = err -> {:halt, err}
      end
    end)
    |> case do
      {:ok, records} -> {:ok, Enum.reverse(records)}
      {:error, _} = err -> err
    end
  end

  defp normalize_batch_get_operate_result(%Aerospike.BatchResult{status: :ok, record: record}),
    do: {:ok, record}

  defp normalize_batch_get_operate_result(%Aerospike.BatchResult{
         status: :error,
         error: %Error{code: code}
       })
       when code in [:key_not_found, :filtered_out, :bin_not_found],
       do: {:ok, nil}

  defp normalize_batch_get_operate_result(%Aerospike.BatchResult{
         status: :error,
         error: %Error{} = err
       }),
       do: {:error, err}

  defp validate_query_ops([]) do
    {:error,
     Error.from_result_code(:parameter_error, message: "query operations cannot be empty")}
  end

  defp validate_query_ops(ops) when is_list(ops) do
    case Enum.find(ops, &(not query_background_write_op?(&1))) do
      nil ->
        :ok

      invalid ->
        {:error,
         Error.from_result_code(:parameter_error,
           message:
             "background query operations must all be write operations, got: #{inspect(invalid)}"
         )}
    end
  end

  defp query_background_write_op?(%Operation{read_header: true}), do: false

  defp query_background_write_op?(%Operation{op_type: op_type}) do
    op_type in [
      Operation.op_write(),
      Operation.op_cdt_modify(),
      Operation.op_add(),
      Operation.op_exp_modify(),
      Operation.op_append(),
      Operation.op_prepend(),
      Operation.op_touch(),
      Operation.op_bit_modify(),
      Operation.op_delete(),
      Operation.op_hll_modify()
    ]
  end

  defp query_background_write_op?(_), do: false

  defp validate_role_names(roles) when is_list(roles) do
    if Enum.all?(roles, &is_binary/1) do
      {:ok, roles}
    else
      {:error,
       Error.from_result_code(:parameter_error,
         message: "roles must be a list of strings"
       )}
    end
  end

  defp validate_privileges(privileges) when is_list(privileges) do
    if Enum.all?(privileges, &match?(%Privilege{}, &1)) do
      {:ok, privileges}
    else
      {:error,
       Error.from_result_code(:parameter_error,
         message: "privileges must be a list of %Aerospike.Privilege{} structs"
       )}
    end
  end

  defp validate_whitelist(whitelist) when is_list(whitelist) do
    if Enum.all?(whitelist, &is_binary/1) do
      {:ok, whitelist}
    else
      {:error,
       Error.from_result_code(:parameter_error,
         message: "whitelist must be a list of strings"
       )}
    end
  end
end
