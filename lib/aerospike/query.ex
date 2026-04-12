defmodule Aerospike.Query do
  @moduledoc """
  Composable secondary-index query description (pure data, no I/O).

  Queries require a namespace and set. Use `where/2` for the secondary-index predicate
  and `filter/2` for additional expression filters (AND-ed at execution time).

  Only one secondary-index filter is kept; `where/2` **replaces** any previous predicate.

  `%Aerospike.Query{}` stays a pure record-selection builder. Execute intent lives in
  explicit facade functions such as `Aerospike.query_stream/3`,
  `Aerospike.query_execute/4`, `Aerospike.query_udf/6`, and
  `Aerospike.query_aggregate/6`.
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
  Starts a query for `namespace` and `set`. Both must be non-empty strings.
  """
  @spec new(String.t(), String.t()) :: t()
  def new(namespace, set) when is_binary(namespace) and is_binary(set) do
    validate_namespace!(namespace)

    if set == "" do
      raise ArgumentError, "set must be a non-empty string for queries"
    end

    %__MODULE__{namespace: namespace, set: set}
  end

  @doc """
  Sets the secondary-index predicate, replacing any previous `where/2` value.
  """
  @spec where(t(), Filter.t()) :: t()
  def where(%__MODULE__{} = query, %Filter{} = filter) do
    %{query | index_filter: filter}
  end

  @doc """
  Restricts returned bins to the given names (strings).
  """
  @spec select(t(), [String.t()]) :: t()
  def select(%__MODULE__{} = query, bin_names) when is_list(bin_names) do
    validate_bin_names!(bin_names)
    %{query | bin_names: bin_names}
  end

  @doc """
  Appends a server-side expression filter (AND-ed with existing filters at execution).
  """
  @spec filter(t(), Exp.t()) :: t()
  def filter(%__MODULE__{} = query, %Exp{} = exp) do
    %{query | filters: query.filters ++ [exp]}
  end

  @doc """
  Sets the maximum number of records to return. Must be a positive integer.
  """
  @spec max_records(t(), pos_integer()) :: t()
  def max_records(%__MODULE__{} = query, n) when is_integer(n) and n > 0 do
    %{query | max_records: n}
  end

  @doc """
  Sets the server-side records-per-second throttle. Use `0` for no throttle.
  """
  @spec records_per_second(t(), non_neg_integer()) :: t()
  def records_per_second(%__MODULE__{} = query, n) when is_integer(n) and n >= 0 do
    %{query | records_per_second: n}
  end

  @doc """
  Attaches a partition filter for partial queries or resume.
  """
  @spec partition_filter(t(), PartitionFilter.t()) :: t()
  def partition_filter(%__MODULE__{} = query, %PartitionFilter{} = pf) do
    %{query | partition_filter: pf}
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

  defp validate_bin_names!(bin_names) do
    Enum.each(bin_names, fn name ->
      unless is_binary(name) and name != "" do
        raise ArgumentError, "bin names must be non-empty strings, got: #{inspect(name)}"
      end
    end)
  end
end
