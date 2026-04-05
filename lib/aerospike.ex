defmodule Aerospike do
  @moduledoc """
  Public API for the Aerospike Elixir client.

  Start a named connection (supervision tree) with `start_link/1`, then pass the
  same `name` atom as the first argument to CRUD functions.

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

      Aerospike.close(:aero)

  """

  alias Aerospike.Batch
  alias Aerospike.BatchOps
  alias Aerospike.CRUD
  alias Aerospike.Error
  alias Aerospike.Key
  alias Aerospike.Policy
  alias Aerospike.Supervisor, as: AeroSupervisor

  @typedoc """
  Named connection handle: currently an atom (`:name` from `start_link/1`).

  Internal routing and ETS table names assume atom registration today. Broader
  `GenServer.server()` support may be added in a future phase.
  """
  @type conn :: atom()

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

  Returns `{:ok, pid}` on success. Fails fast if no seed host is reachable.

  ## Options

  * `:name` — required atom used as `conn` for all operations.
  * `:hosts` — required non-empty list of host strings (`"host:port"` or `"host"` for port 3000).
  * `:pool_size` — connections per node (default `10`).
  * `:pool_checkout_timeout` — pool checkout timeout in ms (default `5000`).
  * `:connect_timeout` — TCP connect timeout in ms (default `5000`).
  * `:tend_interval` — periodic cluster tend interval in ms (default `1000`).
  * `:recv_timeout` — receive timeout for protocol reads in ms (default `5000`).
  * `:auth_opts` — optional authentication keyword list.
  * `:tls` — when `true`, upgrades each node connection with TLS after TCP connect (default `false`).
  * `:tls_opts` — keyword list passed to `:ssl.connect/3` (certificates, verify, SNI, etc.; default `[]`).
    For non-IP hosts, `:server_name_indication` defaults to the hostname unless set in `:tls_opts`.
  * `:defaults` — policy defaults per command (`:write`, `:read`, `:delete`, `:exists`, `:touch`, `:operate`, `:batch`).

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

  Per-call `opts` are merged over connection `defaults` (`Keyword.merge/2`).

  ## Options

  Write policy options: `:ttl`, `:timeout`, `:generation`, `:gen_policy`, `:exists`,
  `:send_key`, `:durable_delete`, `:pool_checkout_timeout`, `:replica`.

  """
  @spec put(conn, Key.t(), map(), keyword()) :: :ok | {:error, Error.t()}
  def put(conn, %Key{} = key, bins, opts \\ []) when is_atom(conn) and is_list(opts) do
    case Policy.validate_write(opts) do
      {:ok, call_opts} ->
        CRUD.put(conn, key, bins, call_opts)

      {:error, %NimbleOptions.ValidationError{} = e} ->
        {:error,
         Error.from_result_code(:parameter_error, message: Policy.validation_error_message(e))}
    end
  end

  @doc """
  Same as `put/4` but returns `:ok` or raises `Aerospike.Error`.
  """
  @spec put!(conn, Key.t(), map(), keyword()) :: :ok
  def put!(conn, %Key{} = key, bins, opts \\ []) when is_atom(conn) and is_list(opts) do
    case put(conn, key, bins, opts) do
      :ok -> :ok
      {:error, %Error{} = e} -> raise e
    end
  end

  @doc """
  Reads a record for the key.

  ## Options

  Read policy: `:timeout`, `:bins`, `:header_only`, `:pool_checkout_timeout`, `:replica`.
  """
  @spec get(conn, Key.t(), keyword()) :: {:ok, Aerospike.Record.t()} | {:error, Error.t()}
  def get(conn, %Key{} = key, opts \\ []) when is_atom(conn) and is_list(opts) do
    case Policy.validate_read(opts) do
      {:ok, call_opts} ->
        CRUD.get(conn, key, call_opts)

      {:error, %NimbleOptions.ValidationError{} = e} ->
        {:error,
         Error.from_result_code(:parameter_error, message: Policy.validation_error_message(e))}
    end
  end

  @doc """
  Same as `get/3` but returns the record or raises `Aerospike.Error`.
  """
  @spec get!(conn, Key.t(), keyword()) :: Aerospike.Record.t()
  def get!(conn, %Key{} = key, opts \\ []) when is_atom(conn) and is_list(opts) do
    case get(conn, key, opts) do
      {:ok, record} -> record
      {:error, %Error{} = e} -> raise e
    end
  end

  @doc """
  Deletes the record. Returns `{:ok, true}` if a record was removed, `{:ok, false}` if absent.

  ## Options

  `:timeout`, `:durable_delete`, `:pool_checkout_timeout`, `:replica`.
  """
  @spec delete(conn, Key.t(), keyword()) :: {:ok, boolean()} | {:error, Error.t()}
  def delete(conn, %Key{} = key, opts \\ []) when is_atom(conn) and is_list(opts) do
    case Policy.validate_delete(opts) do
      {:ok, call_opts} ->
        CRUD.delete(conn, key, call_opts)

      {:error, %NimbleOptions.ValidationError{} = e} ->
        {:error,
         Error.from_result_code(:parameter_error, message: Policy.validation_error_message(e))}
    end
  end

  @doc """
  Same as `delete/3` but returns the boolean or raises `Aerospike.Error`.
  """
  @spec delete!(conn, Key.t(), keyword()) :: boolean()
  def delete!(conn, %Key{} = key, opts \\ []) when is_atom(conn) and is_list(opts) do
    case delete(conn, key, opts) do
      {:ok, deleted?} -> deleted?
      {:error, %Error{} = e} -> raise e
    end
  end

  @doc """
  Returns whether a record exists for the key.

  ## Options

  `:timeout`, `:pool_checkout_timeout`, `:replica`.
  """
  @spec exists(conn, Key.t(), keyword()) :: {:ok, boolean()} | {:error, Error.t()}
  def exists(conn, %Key{} = key, opts \\ []) when is_atom(conn) and is_list(opts) do
    case Policy.validate_exists(opts) do
      {:ok, call_opts} ->
        CRUD.exists(conn, key, call_opts)

      {:error, %NimbleOptions.ValidationError{} = e} ->
        {:error,
         Error.from_result_code(:parameter_error, message: Policy.validation_error_message(e))}
    end
  end

  @doc """
  Same as `exists/3` but returns the boolean or raises `Aerospike.Error`.
  """
  @spec exists!(conn, Key.t(), keyword()) :: boolean()
  def exists!(conn, %Key{} = key, opts \\ []) when is_atom(conn) and is_list(opts) do
    case exists(conn, key, opts) do
      {:ok, exists?} -> exists?
      {:error, %Error{} = e} -> raise e
    end
  end

  @doc """
  Refreshes TTL without changing bins.

  ## Options

  `:ttl`, `:timeout`, `:pool_checkout_timeout`, `:replica`.
  """
  @spec touch(conn, Key.t(), keyword()) :: :ok | {:error, Error.t()}
  def touch(conn, %Key{} = key, opts \\ []) when is_atom(conn) and is_list(opts) do
    case Policy.validate_touch(opts) do
      {:ok, call_opts} ->
        CRUD.touch(conn, key, call_opts)

      {:error, %NimbleOptions.ValidationError{} = e} ->
        {:error,
         Error.from_result_code(:parameter_error, message: Policy.validation_error_message(e))}
    end
  end

  @doc """
  Same as `touch/3` but returns `:ok` or raises `Aerospike.Error`.
  """
  @spec touch!(conn, Key.t(), keyword()) :: :ok
  def touch!(conn, %Key{} = key, opts \\ []) when is_atom(conn) and is_list(opts) do
    case touch(conn, key, opts) do
      :ok -> :ok
      {:error, %Error{} = e} -> raise e
    end
  end

  @doc """
  Runs multiple read/write operations on one record in a single atomic round-trip.

  Pass a list of operations from `Aerospike.Op`, `Aerospike.Op.List`, `Aerospike.Op.Map`, etc.

  ## Options

  Merges **read** and **write** policy keys: `:timeout`, `:ttl`, `:generation`, `:gen_policy`,
  `:exists`, `:send_key`, `:durable_delete`, `:respond_per_each_op`, `:pool_checkout_timeout`,
  `:replica`.
  """
  @spec operate(conn, Key.t(), [Aerospike.Op.t()], keyword()) ::
          {:ok, Aerospike.Record.t()} | {:error, Error.t()}
  def operate(conn, %Key{} = key, ops, opts \\ [])
      when is_atom(conn) and is_list(ops) and is_list(opts) do
    case Policy.validate_operate(opts) do
      {:ok, call_opts} ->
        CRUD.operate(conn, key, ops, call_opts)

      {:error, %NimbleOptions.ValidationError{} = e} ->
        {:error,
         Error.from_result_code(:parameter_error, message: Policy.validation_error_message(e))}
    end
  end

  @doc """
  Same as `operate/4` but returns `%Aerospike.Record{}` or raises `Aerospike.Error`.
  """
  @spec operate!(conn, Key.t(), [Aerospike.Op.t()], keyword()) :: Aerospike.Record.t()
  def operate!(conn, %Key{} = key, ops, opts \\ [])
      when is_atom(conn) and is_list(ops) and is_list(opts) do
    case operate(conn, key, ops, opts) do
      {:ok, record} -> record
      {:error, %Error{} = e} -> raise e
    end
  end

  @doc """
  Atomically adds integer deltas to bins.

  If the record does not exist, Aerospike implicitly creates it — bins start at the
  added value. This makes `add` the idiomatic way to implement counters.

  ## Options

  Write policy options: `:ttl`, `:timeout`, `:generation`, `:gen_policy`, `:exists`,
  `:send_key`, `:durable_delete`, `:pool_checkout_timeout`, `:replica`.

  ## Example

      :ok = Aerospike.add(:aero, key, %{"login_count" => 1, "bytes_used" => 256})

  """
  @spec add(conn, Key.t(), map(), keyword()) :: :ok | {:error, Error.t()}
  def add(conn, %Key{} = key, bins, opts \\ [])
      when is_atom(conn) and is_map(bins) and is_list(opts) do
    case Policy.validate_write(opts) do
      {:ok, call_opts} ->
        CRUD.add(conn, key, bins, call_opts)

      {:error, %NimbleOptions.ValidationError{} = e} ->
        {:error,
         Error.from_result_code(:parameter_error, message: Policy.validation_error_message(e))}
    end
  end

  @doc """
  Same as `add/4` but returns `:ok` or raises `Aerospike.Error`.
  """
  @spec add!(conn, Key.t(), map(), keyword()) :: :ok
  def add!(conn, %Key{} = key, bins, opts \\ [])
      when is_atom(conn) and is_map(bins) and is_list(opts) do
    case add(conn, key, bins, opts) do
      :ok -> :ok
      {:error, %Error{} = e} -> raise e
    end
  end

  @doc """
  Atomically appends string suffixes to bins.

  If the record does not exist, Aerospike implicitly creates it — the bin value
  becomes the appended string (not appended to an empty string).

  ## Options

  Write policy options: `:ttl`, `:timeout`, `:generation`, `:gen_policy`, `:exists`,
  `:send_key`, `:durable_delete`, `:pool_checkout_timeout`, `:replica`.

  ## Example

      :ok = Aerospike.append(:aero, key, %{"greeting" => " world"})

  """
  @spec append(conn, Key.t(), map(), keyword()) :: :ok | {:error, Error.t()}
  def append(conn, %Key{} = key, bins, opts \\ [])
      when is_atom(conn) and is_map(bins) and is_list(opts) do
    case Policy.validate_write(opts) do
      {:ok, call_opts} ->
        CRUD.append(conn, key, bins, call_opts)

      {:error, %NimbleOptions.ValidationError{} = e} ->
        {:error,
         Error.from_result_code(:parameter_error, message: Policy.validation_error_message(e))}
    end
  end

  @doc """
  Same as `append/4` but returns `:ok` or raises `Aerospike.Error`.
  """
  @spec append!(conn, Key.t(), map(), keyword()) :: :ok
  def append!(conn, %Key{} = key, bins, opts \\ [])
      when is_atom(conn) and is_map(bins) and is_list(opts) do
    case append(conn, key, bins, opts) do
      :ok -> :ok
      {:error, %Error{} = e} -> raise e
    end
  end

  @doc """
  Atomically prepends string prefixes to bins.

  If the record does not exist, Aerospike implicitly creates it — the bin value
  becomes the prepended string (not prepended to an empty string).

  ## Options

  Write policy options: `:ttl`, `:timeout`, `:generation`, `:gen_policy`, `:exists`,
  `:send_key`, `:durable_delete`, `:pool_checkout_timeout`, `:replica`.

  ## Example

      :ok = Aerospike.prepend(:aero, key, %{"greeting" => "hello "})

  """
  @spec prepend(conn, Key.t(), map(), keyword()) :: :ok | {:error, Error.t()}
  def prepend(conn, %Key{} = key, bins, opts \\ [])
      when is_atom(conn) and is_map(bins) and is_list(opts) do
    case Policy.validate_write(opts) do
      {:ok, call_opts} ->
        CRUD.prepend(conn, key, bins, call_opts)

      {:error, %NimbleOptions.ValidationError{} = e} ->
        {:error,
         Error.from_result_code(:parameter_error, message: Policy.validation_error_message(e))}
    end
  end

  @doc """
  Same as `prepend/4` but returns `:ok` or raises `Aerospike.Error`.
  """
  @spec prepend!(conn, Key.t(), map(), keyword()) :: :ok
  def prepend!(conn, %Key{} = key, bins, opts \\ [])
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
  @spec batch_get(conn, [Key.t()], keyword()) ::
          {:ok, [Aerospike.Record.t() | nil]} | {:error, Error.t()}
  def batch_get(conn, keys, opts \\ []) when is_atom(conn) and is_list(keys) and is_list(opts) do
    # `Keyword.split/2` returns `{taken_for_these_keys, rest}`.
    {read_kw, batch_kw} = Keyword.split(opts, @batch_read_opts)

    with {:ok, bopts} <- Policy.validate_batch(batch_kw),
         {:ok, ropts} <- Policy.validate_read(read_kw) do
      BatchOps.batch_get(conn, keys, Keyword.merge(bopts, ropts))
    else
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
  @spec batch_get!(conn, [Key.t()], keyword()) :: [Aerospike.Record.t() | nil]
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
  @spec batch_exists(conn, [Key.t()], keyword()) :: {:ok, [boolean()]} | {:error, Error.t()}
  def batch_exists(conn, keys, opts \\ [])
      when is_atom(conn) and is_list(keys) and is_list(opts) do
    case Policy.validate_batch(opts) do
      {:ok, bopts} ->
        BatchOps.batch_exists(conn, keys, bopts)

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
  @spec batch_exists!(conn, [Key.t()], keyword()) :: [boolean()]
  def batch_exists!(conn, keys, opts \\ [])
      when is_atom(conn) and is_list(keys) and is_list(opts) do
    case batch_exists(conn, keys, opts) do
      {:ok, xs} -> xs
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
end
