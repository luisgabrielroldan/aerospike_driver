defmodule Aerospike.Batch do
  @moduledoc """
  Constructors for heterogeneous `Aerospike.batch_operate/3` requests.

  Each function returns an opaque batch operation struct. Combine them in a list and
  pass to `Aerospike.batch_operate/3`. Results come back as `Aerospike.BatchResult`
  structs in the same order.

  ## Example

      alias Aerospike.Batch

      {:ok, results} =
        Aerospike.batch_operate(:aero, [
          Batch.read({"test", "users", "user:1"}, bins: ["name"]),
          Batch.put(key2, %{"x" => 1}),
          Batch.delete(key3)
        ])

  See `Aerospike.batch_get/3` for homogeneous multi-key reads with `nil` for missing keys.

  ## Related

  - `Aerospike.batch_operate/3` — execute a list of batch operations
  - `Aerospike.BatchResult` — per-key result struct
  """

  alias Aerospike.CRUD
  alias Aerospike.Key
  alias Aerospike.Op

  defmodule Read do
    @moduledoc "Batch read operation. Built by `Aerospike.Batch.read/2`."
    @enforce_keys [:key]
    defstruct [:key, :opts]
    @type t :: %__MODULE__{key: Key.t(), opts: keyword()}
  end

  defmodule Put do
    @moduledoc "Batch put operation. Built by `Aerospike.Batch.put/3`."
    @enforce_keys [:key, :bins]
    defstruct [:key, :bins, :opts]
    @type t :: %__MODULE__{key: Key.t(), bins: Aerospike.Record.bins(), opts: keyword()}
  end

  defmodule Delete do
    @moduledoc "Batch delete operation. Built by `Aerospike.Batch.delete/2`."
    @enforce_keys [:key]
    defstruct [:key, :opts]
    @type t :: %__MODULE__{key: Key.t(), opts: keyword()}
  end

  defmodule Operate do
    @moduledoc "Batch multi-op operation. Built by `Aerospike.Batch.operate/3`."
    @enforce_keys [:key, :ops]
    defstruct [:key, :ops, :opts]
    @type t :: %__MODULE__{key: Key.t(), ops: list(Op.t()), opts: keyword()}
  end

  defmodule UDF do
    @moduledoc "Batch UDF invocation. Built by `Aerospike.Batch.udf/5`."
    @enforce_keys [:key, :package, :function]
    defstruct [:key, :package, :function, :args, :opts]

    @type t :: %__MODULE__{
            key: Key.t(),
            package: String.t(),
            function: String.t(),
            args: list(),
            opts: keyword()
          }
  end

  @typedoc """
  One batch operation entry for `Aerospike.batch_operate/3`.

  A batch request can mix reads, writes, deletes, `operate`, and UDF invocations.
  The server returns one `Aerospike.BatchResult.t()` per input entry in input order.
  """
  @type t :: Read.t() | Put.t() | Delete.t() | Operate.t() | UDF.t()

  @doc """
  Returns the record key targeted by a batch operation struct.
  """
  @spec key(t()) :: Key.t()
  def key(%Read{key: k}), do: k
  def key(%Put{key: k}), do: k
  def key(%Delete{key: k}), do: k
  def key(%Operate{key: k}), do: k
  def key(%UDF{key: k}), do: k

  @doc """
  Batch read for one key. Options: `:bins` (list of bin names), `:header_only` (boolean).

  Accepts `%Aerospike.Key{}` or `{namespace, set, user_key}` tuple keys.

  ## Example

      Batch.read(key)                          # read all bins
      Batch.read({"test", "users", "user:1"}) # tuple key form
      Batch.read(key, bins: ["name", "age"])    # project specific bins
      Batch.read(key, header_only: true)        # generation + TTL only

  ## Result codes

  When this operation is used inside `batch_operate/3`, a projected read (`:bins`) on a
  record that exists but does **not** contain a requested bin can surface from the server
  as `:bin_not_found`. The client maps that to `BatchResult` with `status: :ok` and
  `record: nil`, matching the \"missing data\" shape of `batch_get/3` (nil for absent bins /
  missing keys) rather than a per-key error.
  """
  @spec read(Key.key_input(), keyword()) :: Read.t()
  def read(key, opts \\ []) when is_list(opts) do
    key = Key.coerce!(key)
    %Read{key: key, opts: opts}
  end

  @doc """
  Batch put for one key. Bin names may be atoms or strings (normalized to strings).

  Accepts tuple keys; see `read/2`.

  Write-related options: `:ttl`, `:timeout`, `:generation`, `:gen_policy`, `:exists`,
  `:send_key`, `:durable_delete` (merged in `batch_operate`; timeout also applies at batch level).

  ## Example

      Batch.put(key, %{"name" => "Ada", "score" => 42})
      Batch.put(key, %{"temp" => "data"}, ttl: 3600)

  """
  @spec put(Key.key_input(), Aerospike.Record.bins_input(), keyword()) :: Put.t()
  def put(key, bins, opts \\ []) when is_map(bins) and is_list(opts) do
    key = Key.coerce!(key)
    %Put{key: key, bins: CRUD.normalize_bins(bins), opts: opts}
  end

  @doc """
  Batch delete for one key. Options: `:durable_delete`, `:timeout`.

  Accepts tuple keys; see `read/2`.

  ## Example

      Batch.delete(key)
      Batch.delete(key, durable_delete: true)

  """
  @spec delete(Key.key_input(), keyword()) :: Delete.t()
  def delete(key, opts \\ []) when is_list(opts) do
    key = Key.coerce!(key)
    %Delete{key: key, opts: opts}
  end

  @doc """
  Batch atomic multi-operation on one key (same operation list as `Aerospike.operate/4`).

  Accepts tuple keys; see `read/2`.

  Options mirror write/read policies where applicable: `:ttl`, `:timeout`, `:generation`,
  `:gen_policy`, `:exists`, `:send_key`, `:durable_delete`, `:respond_per_each_op`.

  ## Example

      import Aerospike.Op
      Batch.operate(key, [add("hits", 1), get("hits")])

  """
  @spec operate(Key.key_input(), [Op.t()], keyword()) :: Operate.t()
  def operate(key, ops, opts \\ []) when is_list(ops) and is_list(opts) do
    key = Key.coerce!(key)
    %Operate{key: key, ops: ops, opts: opts}
  end

  @doc """
  Batch UDF invocation on one key.

  Accepts tuple keys; see `read/2`.

  `args` is a list of values passed to the server-side function (encoded like other wire values).

  ## Example

      Batch.udf(key, "mymodule", "transform", [1, "x"])
      Batch.udf(key, "aggregate", "sum", ["score"])

  > #### Note {: .info}
  >
  > Registering UDF modules is not part of this phase; ensure the module exists on the
  > server before calling this in production.
  """
  @spec udf(Key.key_input(), String.t(), String.t(), list(), keyword()) :: UDF.t()
  def udf(key, package, function, args \\ [], opts \\ [])
      when is_binary(package) and is_binary(function) and is_list(args) and is_list(opts) do
    key = Key.coerce!(key)
    %UDF{key: key, package: package, function: function, args: args, opts: opts}
  end
end
