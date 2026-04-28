defmodule Aerospike.Scan do
  @moduledoc """
  Composable scan description.

  The scan itself is pure data. It does not own transport state or
  partition iteration.

  Scan execution lives in explicit facade calls such as
  `Aerospike.scan_stream/3`, `Aerospike.scan_all/3`, and
  `Aerospike.scan_count/3`. Node-targeted execution uses `node: node_name`
  in facade opts where supported; discover valid names with
  `Aerospike.node_names/1` or `Aerospike.nodes/1`.

  Filters are server-side expressions. Append expressions with
  `filter/2` to keep returned records scoped by
  `%Aerospike.Exp{}` evaluation.
  """

  alias Aerospike.Exp
  alias Aerospike.PartitionFilter

  @typedoc "Bin names accepted by `select/2`."
  @type bin_names :: [String.t()]

  @enforce_keys [:namespace]
  defstruct [
    :namespace,
    :set,
    bin_names: [],
    filters: [],
    max_records: nil,
    records_per_second: 0,
    partition_filter: nil,
    no_bins: false
  ]

  @typedoc """
  Scan description consumed by `Aerospike.scan_*` helpers.

  `filters` are server-side filter expressions combined with boolean AND by
  the encoder.
  """
  @type t :: %__MODULE__{
          namespace: String.t(),
          set: String.t() | nil,
          bin_names: bin_names(),
          filters: [Exp.t()],
          max_records: pos_integer() | nil,
          records_per_second: non_neg_integer(),
          partition_filter: PartitionFilter.t() | nil,
          no_bins: boolean()
        }

  @doc """
  Starts a namespace-wide scan.

  The namespace must be a non-empty string. Use `new/2` to restrict the scan
  to a set.
  """
  @spec new(String.t()) :: t()
  def new(namespace) when is_binary(namespace) do
    validate_namespace!(namespace)
    %__MODULE__{namespace: namespace, set: nil}
  end

  @doc """
  Starts a scan limited to one set.

  Both namespace and set must be non-empty strings.
  """
  @spec new(String.t(), String.t()) :: t()
  def new(namespace, set) when is_binary(namespace) and is_binary(set) do
    validate_namespace!(namespace)
    validate_set!(set)
    %__MODULE__{namespace: namespace, set: set}
  end

  @doc """
  Restricts returned bin names.

  Passing an empty list keeps the server default of returning all bins unless
  `no_bins/2` or execution opts request otherwise.
  """
  @spec select(t(), bin_names()) :: t()
  def select(%__MODULE__{} = scan, bin_names) when is_list(bin_names) do
    validate_bin_names!(bin_names)
    %{scan | bin_names: bin_names}
  end

  @doc """
  Appends a server-side expression filter.

  The scan encoder appends a single `FILTER_EXP` field for the expression
  filter set.
  """
  @spec filter(t(), Exp.t()) :: t()
  def filter(%__MODULE__{} = scan, %Exp{} = filter) do
    %{scan | filters: scan.filters ++ [filter]}
  end

  @doc """
  Sets the maximum number of records to return.

  Pagination helpers require `max_records` on the scan struct; the returned
  page may contain fewer records when partition progress completes first.
  """
  @spec max_records(t(), pos_integer()) :: t()
  def max_records(%__MODULE__{} = scan, n) when is_integer(n) and n > 0 do
    %{scan | max_records: n}
  end

  @doc """
  Sets the records-per-second throttle.

  `0` disables scan-level throttling.
  """
  @spec records_per_second(t(), non_neg_integer()) :: t()
  def records_per_second(%__MODULE__{} = scan, n) when is_integer(n) and n >= 0 do
    %{scan | records_per_second: n}
  end

  @doc """
  Attaches a partition filter for partial scans or advanced resume.

  The filter is usually built with `Aerospike.PartitionFilter.all/0`,
  `by_id/1`, `by_range/2`, or restored from a page cursor.
  """
  @spec partition_filter(t(), PartitionFilter.t()) :: t()
  def partition_filter(%__MODULE__{} = scan, %PartitionFilter{} = partition_filter) do
    %{scan | partition_filter: partition_filter}
  end

  @doc """
  When `true`, the server omits bin payloads.

  This is useful for count-like scans or key/header-only workflows.
  """
  @spec no_bins(t(), boolean()) :: t()
  def no_bins(%__MODULE__{} = scan, flag) when is_boolean(flag) do
    %{scan | no_bins: flag}
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
