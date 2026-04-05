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
  * `:defaults` — policy defaults per command (`:write`, `:read`, `:delete`, `:exists`, `:touch`, `:operate`).

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
end
