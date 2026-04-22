defmodule Aerospike.ScanOps do
  @moduledoc false

  alias Aerospike.Cursor
  alias Aerospike.Error
  alias Aerospike.ExecuteTask
  alias Aerospike.Page
  alias Aerospike.PartitionFilter
  alias Aerospike.Query
  alias Aerospike.Scan
  alias Aerospike.ScanOps.PageRunner
  alias Aerospike.ScanOps.StreamRunner

  @spec stream(GenServer.server(), Scan.t() | Query.t(), keyword()) ::
          {:ok, Enumerable.t()} | {:error, Error.t()}
  def stream(tender, scannable, opts \\ []) when is_list(opts) do
    with {:ok, node_name, opts2} <- normalize_node_opt(opts) do
      stream_with_node(tender, scannable, node_name, opts2)
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

  @spec query_execute(GenServer.server(), Query.t(), list(), keyword()) ::
          {:ok, ExecuteTask.t()} | {:error, Error.t()}
  def query_execute(tender, %Query{} = query, ops, opts \\ [])
      when is_list(ops) and is_list(opts) do
    with {:ok, node_name, opts2} <- normalize_node_opt(opts) do
      query_execute_with_node(tender, query, ops, node_name, opts2)
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

  @doc false
  @spec allow_record_fold(Aerospike.PartitionTracker.t(), Aerospike.NodePartitions.t(), [term()]) ::
          {Aerospike.PartitionTracker.t(), Aerospike.NodePartitions.t(), [term()]}
  def allow_record_fold(tracker, np, records) when is_list(records) do
    {tracker, np, kept_records_rev} =
      Enum.reduce(records, {tracker, np, []}, fn record, {tracker_acc, np_acc, kept_acc} ->
        case Aerospike.PartitionTracker.allow_record?(tracker_acc, np_acc) do
          {true, tracker2, np2} -> {tracker2, np2, [record | kept_acc]}
          {false, tracker2, np2} -> {tracker2, np2, kept_acc}
        end
      end)

    {tracker, np, Enum.reverse(kept_records_rev)}
  end
end
