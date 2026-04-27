defmodule Aerospike.Batch do
  @moduledoc """
  Constructors for heterogeneous `Aerospike.batch_operate/3` requests.

  Each constructor returns an opaque batch entry value. Pass a list of entries
  to `Aerospike.batch_operate/3` to execute reads, writes, deletes, operations,
  and record UDF calls in one batch request. Results are returned as
  `%Aerospike.BatchResult{}` values in the same order as the input entries.

  Parent batch policy is passed to `Aerospike.batch_operate/3`. Entry-level
  read and write policy opts can be attached to each builder when a field is
  encodable in the batch index protocol.
  """

  alias Aerospike.Key
  alias Aerospike.Record

  defmodule Read do
    @moduledoc "Batch read entry built by `Aerospike.Batch.read/1`."

    @enforce_keys [:key]
    defstruct [:key, :opts]

    @typedoc "Batch read entry targeting one key."
    @type t :: %__MODULE__{key: Key.t(), opts: Aerospike.batch_record_read_opts()}
  end

  defmodule Put do
    @moduledoc "Batch put entry built by `Aerospike.Batch.put/2`."

    @enforce_keys [:key, :bins]
    defstruct [:key, :bins, :opts]

    @typedoc "Batch put entry with bins to write for one key."
    @type t :: %__MODULE__{
            key: Key.t(),
            bins: Record.bins_input(),
            opts: Aerospike.batch_record_write_opts()
          }
  end

  defmodule Delete do
    @moduledoc "Batch delete entry built by `Aerospike.Batch.delete/1`."

    @enforce_keys [:key]
    defstruct [:key, :opts]

    @typedoc "Batch delete entry targeting one key."
    @type t :: %__MODULE__{key: Key.t(), opts: Aerospike.batch_record_write_opts()}
  end

  defmodule Operate do
    @moduledoc "Batch operate entry built by `Aerospike.Batch.operate/2`."

    @enforce_keys [:key, :operations]
    defstruct [:key, :operations, :opts]

    @typedoc "Batch operate entry with record operations for one key."
    @type t :: %__MODULE__{
            key: Key.t(),
            operations: [Aerospike.Op.t()],
            opts: Aerospike.batch_record_read_opts() | Aerospike.batch_record_write_opts()
          }
  end

  defmodule UDF do
    @moduledoc "Batch record-UDF entry built by `Aerospike.Batch.udf/4`."

    @enforce_keys [:key, :package, :function, :args]
    defstruct [:key, :package, :function, :args, :opts]

    @typedoc "Batch record-UDF entry targeting one key."
    @type t :: %__MODULE__{
            key: Key.t(),
            package: String.t(),
            function: String.t(),
            args: list(),
            opts: Aerospike.batch_record_write_opts()
          }
  end

  @typedoc """
  One heterogeneous batch entry for `Aerospike.batch_operate/3`.
  """
  @type t :: Read.t() | Put.t() | Delete.t() | Operate.t() | UDF.t()

  @doc """
  Returns the key targeted by a batch entry.
  """
  @spec key(t()) :: Key.t()
  def key(%Read{key: key}), do: key
  def key(%Put{key: key}), do: key
  def key(%Delete{key: key}), do: key
  def key(%Operate{key: key}), do: key
  def key(%UDF{key: key}), do: key

  @doc """
  Builds a full-record read entry.

  Accepts `%Aerospike.Key{}` values or `{namespace, set, user_key}` tuples.
  """
  @spec read(Key.key_input(), Aerospike.batch_record_read_opts()) :: Read.t()
  def read(key, opts \\ []) when is_list(opts), do: %Read{key: Key.coerce!(key), opts: opts}

  @doc """
  Builds a put entry from a non-empty bin map.

  Bin names may be atoms or strings. Values follow the same supported value
  subset as `Aerospike.put/4`.
  """
  @spec put(Key.key_input(), Record.bins_input(), Aerospike.batch_record_write_opts()) ::
          Put.t()
  def put(key, bins, opts \\ []) when is_map(bins) and is_list(opts) do
    %Put{key: Key.coerce!(key), bins: bins, opts: opts}
  end

  @doc """
  Builds a delete entry.
  """
  @spec delete(Key.key_input(), Aerospike.batch_record_write_opts()) :: Delete.t()
  def delete(key, opts \\ []) when is_list(opts), do: %Delete{key: Key.coerce!(key), opts: opts}

  @doc """
  Builds an operate entry from `Aerospike.Op` operations.
  """
  @spec operate(
          Key.key_input(),
          [Aerospike.Op.t()],
          Aerospike.batch_record_read_opts() | Aerospike.batch_record_write_opts()
        ) :: Operate.t()
  def operate(key, operations, opts \\ []) when is_list(operations) and is_list(opts) do
    %Operate{key: Key.coerce!(key), operations: operations, opts: opts}
  end

  @doc """
  Builds a record-UDF entry.
  """
  @spec udf(Key.key_input(), String.t(), String.t()) :: UDF.t()
  @spec udf(Key.key_input(), String.t(), String.t(), list()) :: UDF.t()
  @spec udf(Key.key_input(), String.t(), String.t(), list(), Aerospike.batch_record_write_opts()) ::
          UDF.t()
  def udf(key, package, function, args \\ [], opts \\ [])
      when is_binary(package) and is_binary(function) and is_list(args) and is_list(opts) do
    %UDF{key: Key.coerce!(key), package: package, function: function, args: args, opts: opts}
  end
end
