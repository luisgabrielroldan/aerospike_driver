defmodule Aerospike.ScanOps do
  @moduledoc false

  alias Aerospike.Cursor
  alias Aerospike.Error
  alias Aerospike.Page
  alias Aerospike.PartitionFilter
  alias Aerospike.Query
  alias Aerospike.Scan
  alias Aerospike.ScanOps.PageRunner
  alias Aerospike.ScanOps.StreamRunner

  @spec stream(GenServer.server(), Scan.t() | Query.t(), keyword()) ::
          {:ok, Enumerable.t()} | {:error, Error.t()}
  def stream(tender, scannable, opts \\ []) when is_list(opts) do
    StreamRunner.stream(tender, scannable, opts)
  end

  @spec stream_node(GenServer.server(), String.t(), Scan.t() | Query.t(), keyword()) ::
          {:ok, Enumerable.t()} | {:error, Error.t()}
  def stream_node(tender, node_name, scannable, opts \\ [])
      when is_binary(node_name) and is_list(opts) do
    StreamRunner.stream_node(tender, node_name, scannable, opts)
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
    collect_node(tender, node_name, scan, opts, &Enum.to_list/1)
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
    collect_node(tender, node_name, scannable, opts, &Enum.count/1)
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

  @spec query_all(GenServer.server(), Query.t(), keyword()) ::
          {:ok, [Aerospike.Record.t()]} | {:error, Error.t()}
  def query_all(tender, %Query{} = query, opts \\ []) when is_list(opts) do
    with :ok <- require_max_records(query) do
      PageRunner.all(tender, query, opts)
    end
  end

  @spec query_all_node(GenServer.server(), String.t(), Query.t(), keyword()) ::
          {:ok, [Aerospike.Record.t()]} | {:error, Error.t()}
  def query_all_node(tender, node_name, %Query{} = query, opts \\ [])
      when is_binary(node_name) and is_list(opts) do
    with :ok <- require_max_records(query) do
      PageRunner.all_node(tender, node_name, query, opts)
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
    {cursor, opts2} = Keyword.pop(opts, :cursor)

    with {:ok, query2} <- apply_optional_cursor(query, cursor),
         :ok <- require_max_records(query2) do
      PageRunner.page(tender, query2, opts2)
    end
  end

  @spec query_page_node(GenServer.server(), String.t(), Query.t(), keyword()) ::
          {:ok, Page.t()} | {:error, Error.t()}
  def query_page_node(tender, node_name, %Query{} = query, opts \\ [])
      when is_binary(node_name) and is_list(opts) do
    {cursor, opts2} = Keyword.pop(opts, :cursor)

    with {:ok, query2} <- apply_optional_cursor(query, cursor),
         :ok <- require_max_records(query2) do
      PageRunner.page_node(tender, node_name, query2, opts2)
    end
  end

  defp collect(tender, scannable, opts, fun) do
    case stream(tender, scannable, opts) do
      {:ok, stream} ->
        try do
          {:ok, fun.(stream)}
        rescue
          err in Aerospike.Error -> {:error, err}
        end

      {:error, %Error{} = err} ->
        {:error, err}
    end
  end

  defp collect_node(tender, node_name, scannable, opts, fun) do
    case stream_node(tender, node_name, scannable, opts) do
      {:ok, stream} ->
        try do
          {:ok, fun.(stream)}
        rescue
          err in Aerospike.Error -> {:error, err}
        end

      {:error, %Error{} = err} ->
        {:error, err}
    end
  end

  defp require_max_records(%Query{max_records: n}) when is_integer(n) and n > 0, do: :ok
  defp require_max_records(%Scan{}), do: :ok
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

  defp attach_cursor_partition_filter(%Scan{} = scan, %Cursor{partitions: partitions}) do
    %{scan | partition_filter: %{PartitionFilter.all() | partitions: partitions}}
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
