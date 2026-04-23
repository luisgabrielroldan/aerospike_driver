defmodule Aerospike.Command.PartitionTracker do
  @moduledoc false

  alias Aerospike.Cursor
  alias Aerospike.Error
  alias Aerospike.Command.NodePartitions
  alias Aerospike.PartitionFilter
  alias Aerospike.Command.PartitionStatus

  @partitions 4_096

  @enforce_keys [
    :partitions,
    :partition_begin,
    :partitions_capacity,
    :node_capacity,
    :replica
  ]
  defstruct [
    :partitions,
    :partition_begin,
    :partitions_capacity,
    :node_capacity,
    :replica,
    node_filter: nil,
    partition_filter: nil,
    node_partitions_list: [],
    record_count: nil,
    max_records: 0,
    max_retries: 5,
    sleep_between_retries: 0,
    sleep_multiplier: 1.0,
    socket_timeout: 0,
    total_timeout: 0,
    iteration: 1,
    deadline_mono_ms: nil,
    exceptions: []
  ]

  @type replica :: :master | :sequence | :any
  @type node_name :: binary()
  @type replica_entry :: node_name() | nil
  @type partition_map :: %{non_neg_integer() => [replica_entry()]}

  @type t :: %__MODULE__{
          partitions: [PartitionStatus.t()],
          partition_begin: non_neg_integer(),
          partitions_capacity: pos_integer(),
          node_capacity: pos_integer(),
          node_filter: node_name() | nil,
          partition_filter: PartitionFilter.t() | nil,
          replica: replica(),
          node_partitions_list: [NodePartitions.t()],
          record_count: non_neg_integer() | nil,
          max_records: non_neg_integer(),
          max_retries: non_neg_integer(),
          sleep_between_retries: non_neg_integer(),
          sleep_multiplier: float(),
          socket_timeout: non_neg_integer(),
          total_timeout: non_neg_integer(),
          iteration: pos_integer(),
          deadline_mono_ms: integer() | nil,
          exceptions: [Error.t()]
        }

  @type completion ::
          {:complete, PartitionFilter.t(), t()}
          | {:continue, t()}
          | {:error, Error.t(), t()}

  @doc """
  Builds a tracker from a partition filter and runtime options.
  """
  @spec new(PartitionFilter.t(), keyword()) :: t()
  def new(%PartitionFilter{} = filter, opts) when is_list(opts) do
    nodes = validate_nodes!(Keyword.fetch!(opts, :nodes))
    max_records = validate_max_records!(Keyword.get(opts, :max_records, 0))
    replica = validate_replica!(Keyword.get(opts, :replica, :sequence))
    node_filter = validate_node_filter!(Keyword.get(opts, :node_filter, nil))

    node_capacity = if node_filter, do: 1, else: length(nodes)
    {statuses, updated_filter} = build_partition_statuses(filter)
    total_timeout = Keyword.get(opts, :total_timeout, 0)
    {socket_timeout, deadline_mono_ms} = compute_timing(total_timeout, opts)

    %__MODULE__{
      partitions: statuses,
      partition_begin: updated_filter.begin,
      partitions_capacity: partitions_capacity(filter, node_capacity),
      node_capacity: node_capacity,
      node_filter: node_filter,
      partition_filter: updated_filter,
      replica: replica,
      max_records: max_records,
      max_retries: Keyword.get(opts, :max_retries, 5),
      sleep_between_retries: Keyword.get(opts, :sleep_between_retries, 0),
      sleep_multiplier: Keyword.get(opts, :sleep_multiplier, 1.0),
      socket_timeout: socket_timeout,
      total_timeout: total_timeout,
      iteration: 1,
      deadline_mono_ms: deadline_mono_ms,
      exceptions: []
    }
  end

  @doc """
  Returns the current resumable cursor or `nil` when the tracker has finished.
  """
  @spec cursor(t()) :: Cursor.t() | nil
  def cursor(%__MODULE__{partition_filter: nil}), do: nil
  def cursor(%__MODULE__{partition_filter: %PartitionFilter{done?: true}}), do: nil

  def cursor(%__MODULE__{partitions: statuses}) do
    %Cursor{partitions: Enum.map(statuses, &project_entry/1)}
  end

  @doc """
  Groups partitions by the node that should serve them.
  """
  @spec assign_partitions_to_nodes(t(), partition_map()) ::
          {:ok, t(), [NodePartitions.t()]} | {:error, Error.t()}
  def assign_partitions_to_nodes(%__MODULE__{} = tracker, partition_map)
      when is_map(partition_map) do
    retry_all =
      case tracker.partition_filter do
        nil -> tracker.iteration == 1
        %PartitionFilter{retry?: true} -> tracker.iteration == 1
        _ -> false
      end

    case route_all(tracker.partitions, tracker, partition_map, retry_all, []) do
      {:ok, list} -> finalize_assignment(tracker, list)
      {:error, %Error{}} = err -> err
    end
  end

  @doc """
  Selects one replica for a partition status using the supplied routing snapshot.
  """
  @spec route_partition(PartitionStatus.t(), replica(), partition_map()) ::
          {:ok, node_name()} | {:error, Error.t()}
  def route_partition(%PartitionStatus{id: id} = ps, replica, partition_map) do
    case Map.fetch(partition_map, id) do
      {:ok, []} ->
        {:error,
         Error.from_result_code(:partition_unavailable,
           message: "No replica nodes for partition #{id}"
         )}

      {:ok, replicas} when is_list(replicas) ->
        select_replica(ps, replica, replicas)

      :error ->
        {:error,
         Error.from_result_code(:partition_unavailable,
           message: "Partition #{id} missing from partition map"
         )}
    end
  end

  @doc """
  Marks a partition as unavailable and advances its replica sequence.
  """
  @spec partition_unavailable(t(), NodePartitions.t(), non_neg_integer()) ::
          {t(), NodePartitions.t()}
  def partition_unavailable(
        %__MODULE__{} = tracker,
        %NodePartitions{} = np,
        partition_id
      )
      when is_integer(partition_id) do
    tracker =
      update_status(tracker, partition_id, fn ps ->
        %{ps | retry?: true, sequence: ps.sequence + 1}
      end)

    np = %{np | parts_unavailable: np.parts_unavailable + 1}
    {tracker, np}
  end

  @doc """
  Records a returned digest for one partition.
  """
  @spec set_digest(t(), NodePartitions.t(), {non_neg_integer(), binary()}) ::
          {t(), NodePartitions.t()}
  def set_digest(%__MODULE__{} = tracker, %NodePartitions{} = np, {partition_id, digest})
      when is_integer(partition_id) and is_binary(digest) do
    tracker = update_status(tracker, partition_id, fn ps -> %{ps | digest: digest} end)
    np = %{np | record_count: np.record_count + 1}
    {tracker, np}
  end

  @doc """
  Records a returned digest and bval for one partition.
  """
  @spec set_last(t(), NodePartitions.t(), non_neg_integer(), {binary(), integer()}) ::
          {t(), NodePartitions.t()}
  def set_last(
        %__MODULE__{} = tracker,
        %NodePartitions{} = np,
        partition_id,
        {digest, bval}
      )
      when is_integer(partition_id) and is_binary(digest) and is_integer(bval) do
    tracker =
      update_status(tracker, partition_id, fn ps -> %{ps | digest: digest, bval: bval} end)

    np = %{np | record_count: np.record_count + 1}
    {tracker, np}
  end

  @doc """
  Applies the tracker-local record budget to one returned record.
  """
  @spec allow_record?(t(), NodePartitions.t()) :: {boolean(), t(), NodePartitions.t()}
  def allow_record?(%__MODULE__{record_count: nil} = tracker, %NodePartitions{} = np) do
    {true, tracker, np}
  end

  def allow_record?(%__MODULE__{record_count: rc} = tracker, %NodePartitions{} = np) do
    new_count = rc + 1

    if new_count <= tracker.max_records do
      {true, %{tracker | record_count: new_count}, np}
    else
      {false, %{tracker | record_count: new_count},
       %{np | disallowed_count: np.disallowed_count + 1}}
    end
  end

  @doc """
  Classifies an error as retryable and advances the node's replica sequence.
  """
  @spec should_retry?(t(), NodePartitions.t(), Error.t()) ::
          {boolean(), t(), NodePartitions.t()}
  def should_retry?(%__MODULE__{} = tracker, %NodePartitions{} = np, %Error{code: code} = err)
      when code in [
             :timeout,
             :network_error,
             :server_not_available,
             :index_not_found,
             :index_not_readable
           ] do
    tracker = mark_retry_sequence(tracker, np)
    np = %{np | parts_unavailable: length(np.parts_full) + length(np.parts_partial)}
    tracker = %{tracker | exceptions: tracker.exceptions ++ [err]}
    {true, tracker, np}
  end

  def should_retry?(%__MODULE__{} = tracker, %NodePartitions{} = np, %Error{}) do
    {false, tracker, np}
  end

  @doc """
  Computes whether the current iteration is complete, should retry, or has failed.
  """
  @spec is_complete?(t(), boolean()) :: completion()
  def is_complete?(%__MODULE__{} = tracker, has_partition_query?)
      when is_boolean(has_partition_query?) do
    totals = aggregate_totals(tracker.node_partitions_list)
    do_is_complete(tracker, totals, has_partition_query?)
  end

  @doc """
  Returns the configured sleep interval between retries.
  """
  @spec should_sleep_for(t()) :: non_neg_integer()
  def should_sleep_for(%__MODULE__{sleep_between_retries: ms}), do: ms

  defp validate_nodes!([]), do: raise(ArgumentError, "nodes list must not be empty")
  defp validate_nodes!(nodes) when is_list(nodes), do: nodes

  defp validate_max_records!(n) when is_integer(n) and n >= 0, do: n

  defp validate_max_records!(other) do
    raise ArgumentError, "max_records must be >= 0, got #{inspect(other)}"
  end

  defp validate_replica!(replica) when replica in [:master, :sequence, :any], do: replica

  defp validate_replica!(other) do
    raise ArgumentError, "replica must be :master, :sequence, or :any, got #{inspect(other)}"
  end

  defp validate_node_filter!(nil), do: nil
  defp validate_node_filter!(node) when is_binary(node), do: node

  defp validate_node_filter!(other) do
    raise ArgumentError, "node_filter must be a binary string or nil, got #{inspect(other)}"
  end

  defp compute_timing(total_timeout, opts) when total_timeout <= 0 do
    {Keyword.get(opts, :socket_timeout, 0), nil}
  end

  defp compute_timing(total_timeout, opts) do
    socket_timeout = Keyword.get(opts, :socket_timeout, 0)

    socket_timeout =
      if socket_timeout == 0 or socket_timeout > total_timeout,
        do: total_timeout,
        else: socket_timeout

    {socket_timeout, System.monotonic_time(:millisecond) + total_timeout}
  end

  defp partitions_capacity(%PartitionFilter{partitions: []} = filter, node_capacity) do
    if filter.count == @partitions do
      ppn = div(@partitions, node_capacity)
      max(1, ppn + div(ppn, 4))
    else
      filter.count
    end
  end

  defp partitions_capacity(%PartitionFilter{} = filter, _node_capacity) do
    filter.count
  end

  defp build_partition_statuses(%PartitionFilter{partitions: []} = filter) do
    statuses =
      Enum.map(0..(filter.count - 1), fn i ->
        status = PartitionStatus.new(filter.begin + i)
        if i == 0 and filter.digest, do: %{status | digest: filter.digest}, else: status
      end)

    updated = %{
      filter
      | partitions: Enum.map(statuses, &project_entry/1),
        retry?: true,
        done?: false
    }

    {statuses, updated}
  end

  defp build_partition_statuses(%PartitionFilter{partitions: entries} = filter)
       when entries != [] do
    statuses = Enum.map(entries, &PartitionStatus.from_entry/1)

    updated = %{
      filter
      | partitions: Enum.map(statuses, &project_entry/1),
        retry?: true,
        done?: false
    }

    {statuses, updated}
  end

  defp route_all([], _tracker, _partition_map, _retry_all, acc), do: {:ok, Enum.reverse(acc)}

  defp route_all([%PartitionStatus{} = ps | rest], tracker, partition_map, retry_all, acc) do
    if retry_all or ps.retry? do
      route_one(ps, rest, tracker, partition_map, retry_all, acc)
    else
      route_all(rest, tracker, partition_map, retry_all, acc)
    end
  end

  defp route_one(ps, rest, tracker, partition_map, retry_all, acc) do
    case route_partition(ps, tracker.replica, partition_map) do
      {:ok, node_name} ->
        append_routed(ps, node_name, rest, tracker, partition_map, retry_all, acc)

      {:error, %Error{}} = err ->
        err
    end
  end

  defp append_routed(
         _ps,
         node_name,
         rest,
         %__MODULE__{node_filter: filter} = tracker,
         partition_map,
         retry_all,
         acc
       )
       when is_binary(filter) and filter != node_name do
    route_all(rest, tracker, partition_map, retry_all, acc)
  end

  defp append_routed(ps, node_name, rest, tracker, partition_map, retry_all, acc) do
    route_all(
      rest,
      tracker,
      partition_map,
      retry_all,
      upsert_np(acc, node_name, %{ps | node: node_name})
    )
  end

  defp upsert_np(list, node_name, %PartitionStatus{} = ps) do
    case Enum.split_with(list, fn np -> np.node == node_name end) do
      {[], _} ->
        [NodePartitions.add_partition(NodePartitions.new(node_name), ps) | list]

      {[np], rest} ->
        [NodePartitions.add_partition(np, ps) | rest]
    end
  end

  defp finalize_assignment(_tracker, []) do
    {:error, Error.from_result_code(:invalid_node, message: "No nodes were assigned")}
  end

  defp finalize_assignment(tracker, list) do
    node_size = length(list)
    {record_count, list} = distribute_record_max(tracker.max_records, node_size, list)

    filter =
      case tracker.partition_filter do
        nil -> nil
        %PartitionFilter{} = f -> %{f | retry?: true}
      end

    tracker = %{
      tracker
      | record_count: record_count,
        node_partitions_list: list,
        partition_filter: filter
    }

    {:ok, tracker, list}
  end

  defp distribute_record_max(0, _node_size, list), do: {nil, list}

  defp distribute_record_max(max_records, node_size, list) when max_records >= node_size do
    base = div(max_records, node_size)
    rem_ = max_records - base * node_size

    list =
      list
      |> Enum.with_index()
      |> Enum.map(fn {np, idx} ->
        record_max = if idx < rem_, do: base + 1, else: base
        %{np | record_max: record_max}
      end)

    {nil, list}
  end

  defp distribute_record_max(_max_records, _node_size, list) do
    list = Enum.map(list, fn np -> %{np | record_max: 1} end)
    {0, list}
  end

  defp select_replica(ps, :sequence, replicas) do
    do_select_sequence_replica(ps.sequence, replicas, length(replicas))
  end

  defp select_replica(_ps, _replica, replicas) do
    case Enum.find(replicas, &is_binary/1) do
      nil ->
        {:error,
         Error.from_result_code(:partition_unavailable,
           message: "No live replica nodes available"
         )}

      node_name ->
        {:ok, node_name}
    end
  end

  defp do_select_sequence_replica(_sequence, _replicas, 0),
    do:
      {:error,
       Error.from_result_code(:partition_unavailable, message: "No live replica nodes available")}

  defp do_select_sequence_replica(sequence, replicas, remaining) when remaining > 0 do
    case Enum.at(replicas, rem(sequence, length(replicas))) do
      node_name when is_binary(node_name) ->
        {:ok, node_name}

      nil ->
        do_select_sequence_replica(sequence + 1, replicas, remaining - 1)
    end
  end

  defp mark_retry(%__MODULE__{} = tracker, %NodePartitions{} = np) do
    ids =
      Enum.map(np.parts_full, & &1.id) ++ Enum.map(np.parts_partial, & &1.id)

    Enum.reduce(ids, tracker, fn id, acc ->
      update_status(acc, id, fn ps -> %{ps | retry?: true} end)
    end)
  end

  defp mark_retry_sequence(%__MODULE__{} = tracker, %NodePartitions{} = np) do
    ids =
      Enum.map(np.parts_full, & &1.id) ++ Enum.map(np.parts_partial, & &1.id)

    Enum.reduce(ids, tracker, fn id, acc ->
      update_status(acc, id, fn ps -> %{ps | retry?: true, sequence: ps.sequence + 1} end)
    end)
  end

  defp update_status(%__MODULE__{partitions: parts} = tracker, partition_id, fun) do
    partitions =
      Enum.map(parts, fn
        %PartitionStatus{id: ^partition_id} = ps -> fun.(ps)
        %PartitionStatus{} = ps -> ps
      end)

    %{tracker | partitions: partitions}
  end

  defp aggregate_totals(list) do
    Enum.reduce(list, {0, 0}, fn np, {rc, pu} ->
      {rc + np.record_count, pu + np.parts_unavailable}
    end)
  end

  defp do_is_complete(tracker, {rec_count, 0}, has_partition_query?) do
    complete_no_unavailable(tracker, rec_count, has_partition_query?)
  end

  defp do_is_complete(tracker, {rec_count, _parts_unavailable}, _hpq) do
    cond do
      tracker.max_records > 0 and rec_count >= tracker.max_records ->
        {:complete, tracker.partition_filter, tracker}

      tracker.iteration > tracker.max_retries ->
        err = Error.from_result_code(:max_retries_exceeded, message: max_retries_message(tracker))
        {:error, err, tracker}

      timed_out?(tracker) ->
        err = Error.from_result_code(:timeout, message: "Total timeout exceeded")
        {:error, err, tracker}

      true ->
        {:continue, advance_iteration(tracker, rec_count)}
    end
  end

  defp complete_no_unavailable(%__MODULE__{max_records: 0} = tracker, _rec_count, _hpq) do
    finish_complete(tracker, done?: true, retry?: false)
  end

  defp complete_no_unavailable(%__MODULE__{iteration: iteration} = tracker, _rec_count, _hpq)
       when iteration > 1 do
    finish_complete(tracker, done?: false, retry?: true)
  end

  defp complete_no_unavailable(%__MODULE__{} = tracker, _rec_count, true) do
    {tracker, done?} =
      Enum.reduce(tracker.node_partitions_list, {tracker, true}, fn np, {acc_tracker, d?} ->
        if np.record_count + np.disallowed_count >= np.record_max do
          {mark_retry(acc_tracker, np), false}
        else
          {acc_tracker, d?}
        end
      end)

    finish_complete(tracker, done?: done?, retry?: false)
  end

  defp complete_no_unavailable(%__MODULE__{} = tracker, _rec_count, false) do
    finish_complete(tracker, done?: false, retry?: false)
  end

  defp finish_complete(%__MODULE__{} = tracker, kw) do
    filter = mark_filter_done(tracker.partition_filter, kw)
    tracker = %{tracker | partition_filter: filter}
    tracker = project_filter_entries(tracker)
    {:complete, tracker.partition_filter, tracker}
  end

  defp mark_filter_done(nil, _kw), do: nil

  defp mark_filter_done(%PartitionFilter{} = filter, kw) do
    %{filter | done?: Keyword.fetch!(kw, :done?), retry?: Keyword.fetch!(kw, :retry?)}
  end

  defp project_filter_entries(%__MODULE__{partition_filter: nil} = tracker), do: tracker

  defp project_filter_entries(
         %__MODULE__{partition_filter: filter, partitions: statuses} = tracker
       ) do
    entries = Enum.map(statuses, &project_entry/1)
    %{tracker | partition_filter: %{filter | partitions: entries}}
  end

  defp project_entry(%PartitionStatus{id: id, digest: digest, bval: bval}) do
    %{id: id, digest: digest, bval: bval}
  end

  defp timed_out?(%__MODULE__{total_timeout: 0}), do: false
  defp timed_out?(%__MODULE__{deadline_mono_ms: nil}), do: false

  defp timed_out?(%__MODULE__{deadline_mono_ms: deadline, sleep_between_retries: sleep}) do
    System.monotonic_time(:millisecond) + sleep >= deadline
  end

  defp advance_iteration(%__MODULE__{} = tracker, rec_count) do
    max_records =
      if tracker.max_records > 0, do: max(tracker.max_records - rec_count, 0), else: 0

    sleep =
      if tracker.sleep_multiplier > 1.0 do
        round(tracker.sleep_between_retries * tracker.sleep_multiplier)
      else
        tracker.sleep_between_retries
      end

    %{
      tracker
      | iteration: tracker.iteration + 1,
        max_records: max_records,
        sleep_between_retries: sleep
    }
  end

  defp max_retries_message(%__MODULE__{max_retries: n, exceptions: []}) do
    "Max retries exceeded: #{n}"
  end

  defp max_retries_message(%__MODULE__{max_retries: n, exceptions: excs}) do
    sub = Enum.map_join(excs, "; ", fn %Error{code: code, message: msg} -> "#{code}: #{msg}" end)
    "Max retries exceeded: #{n} (sub-errors: #{sub})"
  end
end
