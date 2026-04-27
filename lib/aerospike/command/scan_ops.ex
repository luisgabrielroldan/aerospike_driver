defmodule Aerospike.Command.ScanOps do
  @moduledoc false

  alias Aerospike.Command.NodePartitions
  alias Aerospike.Command.PartitionTracker
  alias Aerospike.Command.ScanOps.PageRunner
  alias Aerospike.Command.ScanOps.StreamRunner
  alias Aerospike.Cursor
  alias Aerospike.Error
  alias Aerospike.ExecuteTask
  alias Aerospike.Page
  alias Aerospike.PartitionFilter
  alias Aerospike.Protocol.AsmMsg.Operation
  alias Aerospike.Query
  alias Aerospike.Query.AggregateReducer
  alias Aerospike.Scan

  @spec stream(GenServer.server(), Scan.t() | Query.t(), keyword()) ::
          {:ok, Enumerable.t()} | {:error, Error.t()}
  def stream(tender, scannable, opts \\ []) when is_list(opts) do
    with {:ok, node_name, opts2} <- normalize_node_opt(opts) do
      {cursor, opts3} = Keyword.pop(opts2, :cursor)

      with {:ok, scannable2} <- apply_optional_cursor(scannable, cursor) do
        stream_with_node(tender, scannable2, node_name, opts3)
      end
    end
  end

  @spec stream_node(GenServer.server(), String.t(), Scan.t() | Query.t(), keyword()) ::
          {:ok, Enumerable.t()} | {:error, Error.t()}
  def stream_node(tender, node_name, scannable, opts \\ [])
      when is_binary(node_name) and is_list(opts) do
    with {:ok, opts2} <- put_target_node_opt(opts, node_name) do
      stream(tender, scannable, opts2)
    end
  end

  @spec all(GenServer.server(), Scan.t(), keyword()) ::
          {:ok, [Aerospike.Record.t()]} | {:error, Error.t()}
  def all(tender, %Scan{} = scan, opts \\ []) when is_list(opts) do
    collect(tender, scan, opts, &Enum.to_list/1)
  end

  @spec all_node(GenServer.server(), String.t(), Scan.t(), keyword()) ::
          {:ok, [Aerospike.Record.t()]} | {:error, Error.t()}
  def all_node(tender, node_name, %Scan{} = scan, opts \\ [])
      when is_binary(node_name) and is_list(opts) do
    with {:ok, opts2} <- put_target_node_opt(opts, node_name) do
      all(tender, scan, opts2)
    end
  end

  @spec scan_page(GenServer.server(), Scan.t(), keyword()) ::
          {:ok, Page.t()} | {:error, Error.t()}
  def scan_page(tender, %Scan{} = scan, opts \\ []) when is_list(opts) do
    with {:ok, node_name, opts2} <- normalize_node_opt(opts) do
      {cursor, opts3} = Keyword.pop(opts2, :cursor)

      with {:ok, scan2} <- apply_optional_cursor(scan, cursor),
           :ok <- require_max_records(scan2) do
        scan_page_with_node(tender, scan2, node_name, opts3)
      end
    end
  end

  @spec scan_page_node(GenServer.server(), String.t(), Scan.t(), keyword()) ::
          {:ok, Page.t()} | {:error, Error.t()}
  def scan_page_node(tender, node_name, %Scan{} = scan, opts \\ [])
      when is_binary(node_name) and is_list(opts) do
    with {:ok, opts2} <- put_target_node_opt(opts, node_name) do
      scan_page(tender, scan, opts2)
    end
  end

  @spec count(GenServer.server(), Scan.t() | Query.t(), keyword()) ::
          {:ok, non_neg_integer()} | {:error, Error.t()}
  def count(tender, scannable, opts \\ []) when is_list(opts) do
    collect(tender, scannable, opts, &Enum.count/1)
  end

  @spec count_node(GenServer.server(), String.t(), Scan.t() | Query.t(), keyword()) ::
          {:ok, non_neg_integer()} | {:error, Error.t()}
  def count_node(tender, node_name, scannable, opts \\ [])
      when is_binary(node_name) and is_list(opts) do
    with {:ok, opts2} <- put_target_node_opt(opts, node_name) do
      count(tender, scannable, opts2)
    end
  end

  @spec query_stream(GenServer.server(), Query.t(), keyword()) ::
          {:ok, Enumerable.t()} | {:error, Error.t()}
  def query_stream(tender, %Query{} = query, opts \\ []) when is_list(opts) do
    stream(tender, query, opts)
  end

  @spec query_stream_node(GenServer.server(), String.t(), Query.t(), keyword()) ::
          {:ok, Enumerable.t()} | {:error, Error.t()}
  def query_stream_node(tender, node_name, %Query{} = query, opts \\ [])
      when is_binary(node_name) and is_list(opts) do
    stream_node(tender, node_name, query, opts)
  end

  @spec query_aggregate(GenServer.server(), Query.t(), String.t(), String.t(), list(), keyword()) ::
          {:ok, Enumerable.t()} | {:error, Error.t()}
  def query_aggregate(tender, %Query{} = query, package, function, args, opts \\ [])
      when is_binary(package) and is_binary(function) and is_list(args) and is_list(opts) do
    StreamRunner.query_aggregate(tender, query, opts, package, function, args)
  end

  @spec query_aggregate_result(
          GenServer.server(),
          Query.t(),
          String.t(),
          String.t(),
          list(),
          keyword()
        ) :: {:ok, term() | nil} | {:error, Error.t()}
  def query_aggregate_result(tender, %Query{} = query, package, function, args, opts \\ [])
      when is_binary(package) and is_binary(function) and is_list(args) and is_list(opts) do
    with {:ok, reducer} <- AggregateReducer.prepare(package, function, args, opts),
         {:ok, stream} <-
           query_aggregate(tender, query, package, function, args, aggregate_transport_opts(opts)) do
      AggregateReducer.run(reducer, stream)
    end
  end

  @spec query_execute(GenServer.server(), Query.t(), list(), keyword()) ::
          {:ok, ExecuteTask.t()} | {:error, Error.t()}
  def query_execute(tender, %Query{} = query, ops, opts \\ [])
      when is_list(ops) and is_list(opts) do
    with {:ok, built_ops} <- build_background_operations(ops),
         {:ok, node_name, opts2} <- normalize_node_opt(opts) do
      query_execute_with_node(tender, query, built_ops, node_name, opts2)
    end
  end

  @spec query_execute_node(GenServer.server(), String.t(), Query.t(), list(), keyword()) ::
          {:ok, ExecuteTask.t()} | {:error, Error.t()}
  def query_execute_node(tender, node_name, %Query{} = query, ops, opts \\ [])
      when is_binary(node_name) and is_list(ops) and is_list(opts) do
    with {:ok, opts2} <- put_target_node_opt(opts, node_name) do
      query_execute(tender, query, ops, opts2)
    end
  end

  @spec query_udf(GenServer.server(), Query.t(), String.t(), String.t(), list(), keyword()) ::
          {:ok, ExecuteTask.t()} | {:error, Error.t()}
  def query_udf(tender, %Query{} = query, package, function, args, opts \\ [])
      when is_binary(package) and is_binary(function) and is_list(args) and is_list(opts) do
    with {:ok, node_name, opts2} <- normalize_node_opt(opts) do
      query_udf_with_node(tender, query, package, function, args, node_name, opts2)
    end
  end

  @spec query_udf_node(
          GenServer.server(),
          String.t(),
          Query.t(),
          String.t(),
          String.t(),
          list(),
          keyword()
        ) :: {:ok, ExecuteTask.t()} | {:error, Error.t()}
  def query_udf_node(tender, node_name, %Query{} = query, package, function, args, opts \\ [])
      when is_binary(node_name) and is_binary(package) and is_binary(function) and is_list(args) and
             is_list(opts) do
    with {:ok, opts2} <- put_target_node_opt(opts, node_name) do
      query_udf(tender, query, package, function, args, opts2)
    end
  end

  @spec query_all(GenServer.server(), Query.t(), keyword()) ::
          {:ok, [Aerospike.Record.t()]} | {:error, Error.t()}
  def query_all(tender, %Query{} = query, opts \\ []) when is_list(opts) do
    with :ok <- require_max_records(query),
         {:ok, node_name, opts2} <- normalize_node_opt(opts) do
      query_all_with_node(tender, query, node_name, opts2)
    end
  end

  @spec query_all_node(GenServer.server(), String.t(), Query.t(), keyword()) ::
          {:ok, [Aerospike.Record.t()]} | {:error, Error.t()}
  def query_all_node(tender, node_name, %Query{} = query, opts \\ [])
      when is_binary(node_name) and is_list(opts) do
    with {:ok, opts2} <- put_target_node_opt(opts, node_name) do
      query_all(tender, query, opts2)
    end
  end

  @spec query_count(GenServer.server(), Query.t(), keyword()) ::
          {:ok, non_neg_integer()} | {:error, Error.t()}
  def query_count(tender, %Query{} = query, opts \\ []) when is_list(opts) do
    count(tender, query, opts)
  end

  @spec query_count_node(GenServer.server(), String.t(), Query.t(), keyword()) ::
          {:ok, non_neg_integer()} | {:error, Error.t()}
  def query_count_node(tender, node_name, %Query{} = query, opts \\ [])
      when is_binary(node_name) and is_list(opts) do
    count_node(tender, node_name, query, opts)
  end

  @spec query_page(GenServer.server(), Query.t(), keyword()) ::
          {:ok, Page.t()} | {:error, Error.t()}
  def query_page(tender, %Query{} = query, opts \\ []) when is_list(opts) do
    with {:ok, node_name, opts2} <- normalize_node_opt(opts) do
      {cursor, opts3} = Keyword.pop(opts2, :cursor)

      with {:ok, query2} <- apply_optional_cursor(query, cursor),
           :ok <- require_max_records(query2) do
        query_page_with_node(tender, query2, node_name, opts3)
      end
    end
  end

  @spec query_page_node(GenServer.server(), String.t(), Query.t(), keyword()) ::
          {:ok, Page.t()} | {:error, Error.t()}
  def query_page_node(tender, node_name, %Query{} = query, opts \\ [])
      when is_binary(node_name) and is_list(opts) do
    with {:ok, opts2} <- put_target_node_opt(opts, node_name) do
      query_page(tender, query, opts2)
    end
  end

  defp collect(tender, scannable, opts, fun) do
    with {:ok, node_name, opts2} <- normalize_node_opt(opts),
         {:ok, stream} <- stream_with_node(tender, scannable, node_name, opts2) do
      try do
        {:ok, fun.(stream)}
      rescue
        err in Aerospike.Error -> {:error, err}
      end
    end
  end

  defp query_all_with_node(tender, query, nil, opts), do: PageRunner.all(tender, query, opts)

  defp query_all_with_node(tender, query, node_name, opts),
    do: PageRunner.all_node(tender, node_name, query, opts)

  defp scan_page_with_node(tender, scan, nil, opts), do: PageRunner.page(tender, scan, opts)

  defp scan_page_with_node(tender, scan, node_name, opts),
    do: PageRunner.page_node(tender, node_name, scan, opts)

  defp query_page_with_node(tender, query, nil, opts), do: PageRunner.page(tender, query, opts)

  defp query_page_with_node(tender, query, node_name, opts),
    do: PageRunner.page_node(tender, node_name, query, opts)

  defp stream_with_node(tender, scannable, nil, opts),
    do: StreamRunner.stream(tender, scannable, opts)

  defp stream_with_node(tender, scannable, node_name, opts),
    do: StreamRunner.stream_node(tender, node_name, scannable, opts)

  defp query_execute_with_node(tender, query, ops, nil, opts),
    do: StreamRunner.query_execute(tender, query, ops, opts)

  defp query_execute_with_node(tender, query, ops, node_name, opts),
    do: StreamRunner.query_execute_node(tender, node_name, query, ops, opts)

  defp query_udf_with_node(tender, query, package, function, args, nil, opts),
    do: StreamRunner.query_udf(tender, query, package, function, args, opts)

  defp query_udf_with_node(tender, query, package, function, args, node_name, opts),
    do: StreamRunner.query_udf_node(tender, node_name, query, package, function, args, opts)

  defp aggregate_transport_opts(opts) do
    Keyword.drop(opts, [:source, :source_path])
  end

  defp build_background_operations(operations) do
    operations
    |> Enum.reduce_while({:ok, []}, fn operation, {:ok, acc} ->
      case build_background_operation(operation) do
        {:ok, built_operation} -> {:cont, {:ok, [built_operation | acc]}}
        {:error, %Error{}} = err -> {:halt, err}
      end
    end)
    |> case do
      {:ok, built_operations} -> {:ok, Enum.reverse(built_operations)}
      {:error, %Error{}} = err -> err
    end
  end

  defp build_background_operation(%Operation{} = operation) do
    validate_background_write_operation({:ok, operation}, operation)
  end

  defp build_background_operation({:read, bin_name} = operation) do
    {:read, normalize_bin_name(bin_name)}
    |> Operation.from_simple()
    |> validate_background_write_operation(operation)
  end

  defp build_background_operation({kind, bin_name, value} = operation)
       when kind in [:write, :add, :append, :prepend] do
    {kind, normalize_bin_name(bin_name), value}
    |> Operation.from_simple()
    |> validate_background_write_operation(operation)
  end

  defp build_background_operation(operation) do
    operation
    |> Operation.from_simple()
    |> validate_background_write_operation(operation)
  end

  defp validate_background_write_operation({:ok, %Operation{} = operation}, source) do
    if background_write_operation?(operation) do
      {:ok, operation}
    else
      invalid_background_operation(source)
    end
  end

  defp validate_background_write_operation({:error, %Error{}} = err, _source), do: err

  defp background_write_operation?(%Operation{read_header: true}), do: false

  defp background_write_operation?(%Operation{op_type: op_type}) do
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

  defp invalid_background_operation(operation) do
    {:error,
     Error.from_result_code(:invalid_argument,
       message:
         "background query operations must all be write operations, got: #{inspect(operation)}"
     )}
  end

  defp normalize_bin_name(bin_name) when is_atom(bin_name), do: Atom.to_string(bin_name)
  defp normalize_bin_name(bin_name), do: bin_name

  defp normalize_node_opt(opts) when is_list(opts) do
    case Keyword.pop(opts, :node) do
      {nil, opts2} ->
        {:ok, nil, opts2}

      {node_name, opts2} when is_binary(node_name) ->
        {:ok, node_name, opts2}

      {other, _opts2} ->
        {:error,
         Error.from_result_code(:parameter_error,
           message: "invalid node option: #{inspect(other)}"
         )}
    end
  end

  defp put_target_node_opt(opts, node_name) when is_list(opts) and is_binary(node_name) do
    case Keyword.has_key?(opts, :node) do
      true ->
        {:error,
         Error.from_result_code(
           :parameter_error,
           message: "node option conflicts with the positional node argument"
         )}

      false ->
        {:ok, Keyword.put(opts, :node, node_name)}
    end
  end

  defp require_max_records(%Query{max_records: n}) when is_integer(n) and n > 0, do: :ok
  defp require_max_records(%Scan{max_records: n}) when is_integer(n) and n > 0, do: :ok
  defp require_max_records(_), do: {:error, Error.from_result_code(:max_records_required)}

  defp apply_optional_cursor(scannable, nil), do: {:ok, scannable}

  defp apply_optional_cursor(scannable, %Cursor{} = cursor),
    do: {:ok, attach_cursor_partition_filter(scannable, cursor)}

  defp apply_optional_cursor(scannable, bin) when is_binary(bin) do
    case Cursor.decode(bin) do
      {:ok, cursor} -> {:ok, attach_cursor_partition_filter(scannable, cursor)}
      {:error, _} = err -> err
    end
  end

  defp apply_optional_cursor(_scannable, other) do
    {:error,
     Error.from_result_code(:parameter_error, message: "invalid cursor: #{inspect(other)}")}
  end

  defp attach_cursor_partition_filter(%Query{} = query, %Cursor{partitions: partitions}) do
    %{query | partition_filter: %{PartitionFilter.all() | partitions: partitions}}
  end

  defp attach_cursor_partition_filter(%Scan{} = scan, %Cursor{partitions: partitions}) do
    %{scan | partition_filter: %{PartitionFilter.all() | partitions: partitions}}
  end

  @doc false
  @spec allow_record_fold(
          PartitionTracker.t(),
          NodePartitions.t(),
          [term()]
        ) ::
          {PartitionTracker.t(), NodePartitions.t(), [term()]}
  def allow_record_fold(tracker, np, records) when is_list(records) do
    {tracker, np, kept_records_rev} =
      Enum.reduce(records, {tracker, np, []}, fn record, {tracker_acc, np_acc, kept_acc} ->
        case PartitionTracker.allow_record?(tracker_acc, np_acc) do
          {true, tracker2, np2} -> {tracker2, np2, [record | kept_acc]}
          {false, tracker2, np2} -> {tracker2, np2, kept_acc}
        end
      end)

    {tracker, np, Enum.reverse(kept_records_rev)}
  end
end
