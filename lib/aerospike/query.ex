defmodule Aerospike.Query do
  @moduledoc """
  Composable query description with separate predicate lanes.

  The query itself is pure data. It does not own transport state or
  partition iteration.

  Query execution lives in explicit facade calls such as
  `Aerospike.query_stream/3`, `Aerospike.query_aggregate/6`,
  `Aerospike.query_aggregate_result/6`, `Aerospike.query_execute/4`, and
  `Aerospike.query_udf/6`.
  Node-targeted execution uses `node: node_name` in facade opts where that
  query helper supports it. Discover valid names with
  `Aerospike.node_names/1` or `Aerospike.nodes/1`. Finalized aggregate
  queries do not support node targeting because they must consume every server
  partial needed for one local final result. `query_all/3` and `query_page/3`
  require `query.max_records`.

  Use `where/2` for secondary-index predicates (`Aerospike.Filter`), and
  `filter/2` for server-side expression filters (`Aerospike.Exp`). Both can be
  used together where the query is supported.
  """

  alias Aerospike.Exp
  alias Aerospike.Filter
  alias Aerospike.PartitionFilter

  @enforce_keys [:namespace, :set]
  defstruct [
    :namespace,
    :set,
    :index_filter,
    bin_names: [],
    filters: [],
    max_records: nil,
    records_per_second: 0,
    partition_filter: nil,
    no_bins: false
  ]

  @typedoc """
  Query description consumed by `Aerospike.query_*` helpers.

  `index_filter` is the single secondary-index query filter. `filters` are
  server-side filter expressions combined with boolean AND by the encoder.
  """
  @type t :: %__MODULE__{
          namespace: String.t(),
          set: String.t(),
          index_filter: Filter.t() | nil,
          bin_names: [String.t()],
          filters: [Exp.t()],
          max_records: pos_integer() | nil,
          records_per_second: non_neg_integer(),
          partition_filter: PartitionFilter.t() | nil,
          no_bins: boolean()
        }

  @doc """
  Starts a query for a namespace and set.
  """
  @spec new(String.t(), String.t()) :: t()
  def new(namespace, set) when is_binary(namespace) and is_binary(set) do
    validate_namespace!(namespace)
    validate_set!(set)
    %__MODULE__{namespace: namespace, set: set}
  end

  @doc """
  Sets the secondary-index predicate.
  """
  @spec where(t(), Filter.t()) :: t()
  def where(%__MODULE__{} = query, %Filter{} = index_filter) do
    %{query | index_filter: index_filter}
  end

  @doc """
  Restricts returned bin names.
  """
  @spec select(t(), [String.t()]) :: t()
  def select(%__MODULE__{} = query, bin_names) when is_list(bin_names) do
    validate_bin_names!(bin_names)
    %{query | bin_names: bin_names}
  end

  @doc """
  Appends a server-side expression filter.

  Multiple calls append additional expression filters that are combined with
  server-side boolean AND when encoded.
  """
  @spec filter(t(), Exp.t()) :: t()
  def filter(%__MODULE__{} = query, %Exp{} = filter) do
    %{query | filters: query.filters ++ [filter]}
  end

  @doc """
  Sets the maximum number of records to return.
  """
  @spec max_records(t(), pos_integer()) :: t()
  def max_records(%__MODULE__{} = query, n) when is_integer(n) and n > 0 do
    %{query | max_records: n}
  end

  @doc """
  Sets the records-per-second throttle.
  """
  @spec records_per_second(t(), non_neg_integer()) :: t()
  def records_per_second(%__MODULE__{} = query, n) when is_integer(n) and n >= 0 do
    %{query | records_per_second: n}
  end

  @doc """
  Attaches a partition filter for partial queries or advanced resume.
  """
  @spec partition_filter(t(), PartitionFilter.t()) :: t()
  def partition_filter(%__MODULE__{} = query, %PartitionFilter{} = partition_filter) do
    %{query | partition_filter: partition_filter}
  end

  @doc """
  When `true`, the server omits bin payloads.
  """
  @spec no_bins(t(), boolean()) :: t()
  def no_bins(%__MODULE__{} = query, flag) when is_boolean(flag) do
    %{query | no_bins: flag}
  end

  defp validate_namespace!(namespace) do
    if namespace == "" do
      raise ArgumentError, "namespace must be a non-empty string"
    end
  end

  defp validate_set!(set) do
    if set == "" do
      raise ArgumentError, "set must be a non-empty string"
    end
  end

  defp validate_bin_names!(bin_names) do
    Enum.each(bin_names, fn name ->
      unless is_binary(name) and name != "" do
        raise ArgumentError, "bin names must be non-empty strings, got: #{inspect(name)}"
      end
    end)
  end
end
