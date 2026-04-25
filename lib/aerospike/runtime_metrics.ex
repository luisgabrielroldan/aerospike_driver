defmodule Aerospike.RuntimeMetrics do
  @moduledoc false

  alias Aerospike.Cluster
  alias Aerospike.Cluster.Tender
  alias Aerospike.Error

  @metrics_enabled_key {:runtime, :metrics_enabled}

  @spec init(atom() | pid(), keyword()) :: :ok
  def init(cluster, opts \\ []) when is_list(opts) do
    with {:ok, table} <- meta_table(cluster) do
      :ets.insert_new(table, {@metrics_enabled_key, false})
      maybe_put_config(table, :pool_size, Keyword.get(opts, :pool_size))
      maybe_put_config(table, :tend_interval_ms, Keyword.get(opts, :tend_interval_ms))
    end

    :ok
  end

  @spec metrics_enabled?(atom() | pid()) :: boolean()
  def metrics_enabled?(cluster) do
    case lookup_meta(cluster, @metrics_enabled_key) do
      [{@metrics_enabled_key, enabled?}] -> enabled?
      _ -> false
    end
  end

  @spec enable(atom() | pid(), keyword()) :: :ok | {:error, Error.t()}
  def enable(cluster, opts \\ []) when is_list(opts) do
    with {:ok, table} <- meta_table(cluster) do
      if Keyword.get(opts, :reset, false) do
        clear_runtime_counters(table)
      end

      :ets.insert(table, {@metrics_enabled_key, true})
      :ok
    else
      :error -> {:error, Error.from_result_code(:cluster_not_ready)}
    end
  end

  @spec disable(atom() | pid()) :: :ok | {:error, Error.t()}
  def disable(cluster) do
    with {:ok, table} <- meta_table(cluster) do
      :ets.insert(table, {@metrics_enabled_key, false})
      :ok
    else
      :error -> {:error, Error.from_result_code(:cluster_not_ready)}
    end
  end

  @spec stats(atom() | pid()) :: map()
  def stats(cluster) do
    case meta_table(cluster) do
      {:ok, table} ->
        meta_entries = Map.new(:ets.tab2list(table))
        node_rows = current_node_rows(cluster, meta_entries)

        commands = command_stats(meta_entries)
        retries = retry_stats(meta_entries)
        cluster_connections = connection_stats(meta_entries, nil)

        %{
          metrics_enabled: metrics_enabled?(cluster),
          cluster_ready: Map.get(meta_entries, :ready, false),
          nodes_total: map_size(node_rows),
          nodes_active: Enum.count(node_rows, fn {_name, row} -> row.active end),
          open_connections: Map.get(cluster_connections, :open, 0),
          commands_total: Map.get(commands, :total, 0),
          commands_ok: Map.get(commands, :ok, 0),
          commands_error: Map.get(commands, :error, 0),
          cluster: %{
            config: %{
              pool_size: lookup_config(meta_entries, :pool_size),
              tend_interval_ms: lookup_config(meta_entries, :tend_interval_ms)
            },
            tends: %{
              total: cluster_counter(meta_entries, :tends_total),
              successful: cluster_counter(meta_entries, :tends_successful),
              failed: cluster_counter(meta_entries, :tends_failed)
            },
            nodes: %{
              added: cluster_counter(meta_entries, :node_added_count),
              removed: cluster_counter(meta_entries, :node_removed_count)
            },
            partition_map_updates: cluster_counter(meta_entries, :partition_map_updates),
            retries: retries,
            connections: cluster_connections,
            commands: commands
          },
          nodes:
            Map.new(node_rows, fn {node_name, row} ->
              {node_name,
               %{
                 active: row.active,
                 connections: connection_stats(meta_entries, node_name)
               }}
            end)
        }

      :error ->
        default_stats()
    end
  end

  @spec record_command(atom() | pid(), module(), integer(), term()) :: :ok
  def record_command(cluster, command, start_mono, result)
      when is_atom(command) and is_integer(start_mono) do
    maybe_record(cluster, fn table ->
      duration_us =
        System.convert_time_unit(System.monotonic_time() - start_mono, :native, :microsecond)

      bump_counter(table, {:runtime, :commands, :total})
      bump_counter(table, {:runtime, :commands, :latency_us_total}, duration_us)
      bump_counter(table, {:runtime, :command, command, :total})
      bump_counter(table, {:runtime, :command, command, :latency_us_total}, duration_us)

      case command_result_tag(result) do
        :ok ->
          bump_counter(table, {:runtime, :commands, :ok})
          bump_counter(table, {:runtime, :command, command, :ok})

        {:error, code} ->
          bump_counter(table, {:runtime, :commands, :error})
          bump_counter(table, {:runtime, :command, command, :error})
          bump_counter(table, {:runtime, :error_code, code})

        _ ->
          :ok
      end
    end)
  end

  @spec record_retry_attempt(atom() | pid(), :rebalance | :transport | :circuit_open) :: :ok
  def record_retry_attempt(cluster, classification)
      when classification in [:rebalance, :transport, :circuit_open] do
    maybe_record(cluster, fn table ->
      bump_counter(table, {:runtime, :retries, :total})
      bump_counter(table, {:runtime, :retries, classification})
    end)
  end

  @spec record_tend(atom() | pid(), :ok | :error) :: :ok
  def record_tend(cluster, result) when result in [:ok, :error] do
    maybe_record(cluster, fn table ->
      bump_counter(table, {:runtime, :cluster, :tends_total})

      case result do
        :ok -> bump_counter(table, {:runtime, :cluster, :tends_successful})
        :error -> bump_counter(table, {:runtime, :cluster, :tends_failed})
      end
    end)
  end

  @spec record_partition_map_update(atom() | pid()) :: :ok
  def record_partition_map_update(cluster) do
    maybe_record(cluster, &bump_counter(&1, {:runtime, :cluster, :partition_map_updates}))
  end

  @spec record_node_added(atom() | pid(), String.t()) :: :ok
  def record_node_added(cluster, node_name) when is_binary(node_name) do
    maybe_record(cluster, fn table ->
      bump_counter(table, {:runtime, :cluster, :node_added_count})
      bump_counter(table, {:runtime, :node, node_name, :seen})
    end)
  end

  @spec record_node_removed(atom() | pid(), String.t()) :: :ok
  def record_node_removed(cluster, node_name) when is_binary(node_name) do
    maybe_record(cluster, fn table ->
      bump_counter(table, {:runtime, :cluster, :node_removed_count})
      bump_counter(table, {:runtime, :node, node_name, :seen})
    end)
  end

  @spec record_connection_attempt(atom() | pid() | nil, String.t() | nil) :: :ok
  def record_connection_attempt(nil, _node_name), do: :ok

  def record_connection_attempt(cluster, node_name) do
    maybe_record(cluster, fn table ->
      bump_connection(table, node_name, :attempts)
    end)
  end

  @spec record_connection_success(atom() | pid() | nil, String.t() | nil) :: :ok
  def record_connection_success(nil, _node_name), do: :ok

  def record_connection_success(cluster, node_name) do
    maybe_record(cluster, fn table ->
      bump_connection(table, node_name, :successful)
    end)
  end

  @spec record_connection_failure(atom() | pid() | nil, String.t() | nil) :: :ok
  def record_connection_failure(nil, _node_name), do: :ok

  def record_connection_failure(cluster, node_name) do
    maybe_record(cluster, fn table ->
      bump_connection(table, node_name, :failed)
    end)
  end

  @spec record_connection_closed(atom() | pid() | nil, String.t() | nil) :: :ok
  def record_connection_closed(nil, _node_name), do: :ok

  def record_connection_closed(cluster, node_name) do
    maybe_record(cluster, fn table ->
      bump_connection(table, node_name, :closed)
    end)
  end

  @spec record_connection_drop(atom() | pid() | nil, String.t() | nil, :idle | :dead) :: :ok
  def record_connection_drop(nil, _node_name, _reason), do: :ok

  def record_connection_drop(cluster, node_name, :idle) do
    maybe_record(cluster, fn table ->
      bump_connection(table, node_name, :idle_dropped)
    end)
  end

  def record_connection_drop(cluster, node_name, :dead) do
    maybe_record(cluster, fn table ->
      bump_connection(table, node_name, :dead_dropped)
    end)
  end

  @spec record_checkout_failure(
          atom() | pid() | nil,
          String.t() | nil,
          :pool_timeout | :network_error
        ) ::
          :ok
  def record_checkout_failure(nil, _node_name, _reason), do: :ok

  def record_checkout_failure(cluster, node_name, :pool_timeout) do
    maybe_record(cluster, fn table ->
      bump_connection(table, node_name, :pool_timeouts)
    end)
  end

  def record_checkout_failure(cluster, node_name, :network_error) do
    maybe_record(cluster, fn table ->
      bump_connection(table, node_name, :network_errors)
    end)
  end

  defp maybe_record(cluster, fun) do
    case meta_table(cluster) do
      {:ok, table} ->
        if metrics_enabled?(cluster) do
          fun.(table)
        end

      :error ->
        :ok
    end

    :ok
  end

  defp meta_table(cluster) do
    table =
      cluster
      |> Cluster.tables()
      |> Map.fetch!(:meta)

    if :ets.whereis(table) == :undefined do
      :error
    else
      {:ok, table}
    end
  rescue
    _ -> :error
  end

  defp lookup_meta(cluster, key) do
    with {:ok, table} <- meta_table(cluster) do
      :ets.lookup(table, key)
    else
      :error -> []
    end
  end

  defp clear_runtime_counters(table) do
    table
    |> :ets.tab2list()
    |> Enum.each(fn
      {@metrics_enabled_key, _value} ->
        :ok

      {{:runtime, :config, _key}, _value} ->
        :ok

      {key, _value} = entry when is_tuple(key) and tuple_size(key) > 0 ->
        if elem(key, 0) == :runtime do
          :ets.delete_object(table, entry)
        end

      _entry ->
        :ok
    end)
  end

  defp current_node_rows(cluster, meta_entries) do
    active_nodes =
      case tender_pid(cluster) do
        nil -> []
        pid -> pid |> Tender.nodes_status() |> Enum.flat_map(&active_node_name/1)
      end

    known_nodes =
      meta_entries
      |> Map.keys()
      |> Enum.flat_map(&known_node_name/1)
      |> MapSet.new()
      |> MapSet.union(MapSet.new(active_nodes))

    Map.new(known_nodes, fn node_name ->
      {node_name, %{active: node_name in active_nodes}}
    end)
  end

  defp tender_pid(cluster) when is_pid(cluster), do: cluster

  defp tender_pid(cluster) when is_atom(cluster) do
    Process.whereis(cluster)
  end

  defp tender_pid(_cluster), do: nil

  defp active_node_name({node_name, %{status: :active}}), do: [node_name]
  defp active_node_name(_entry), do: []

  defp known_node_name({:runtime, :node, node_name, _metric}) when is_binary(node_name),
    do: [node_name]

  defp known_node_name(_entry), do: []

  defp command_stats(meta_entries) do
    by_command =
      Enum.reduce(meta_entries, %{}, fn
        {{:runtime, :command, command, metric}, value}, acc ->
          Map.update(acc, command, %{metric => value}, &Map.put(&1, metric, value))

        _, acc ->
          acc
      end)

    errors_by_code =
      Enum.reduce(meta_entries, %{}, fn
        {{:runtime, :error_code, code}, value}, acc ->
          Map.put(acc, code, value)

        _, acc ->
          acc
      end)

    %{
      total: Map.get(meta_entries, {:runtime, :commands, :total}, 0),
      ok: Map.get(meta_entries, {:runtime, :commands, :ok}, 0),
      error: Map.get(meta_entries, {:runtime, :commands, :error}, 0),
      latency_us_total: Map.get(meta_entries, {:runtime, :commands, :latency_us_total}, 0),
      by_command: by_command,
      errors_by_code: errors_by_code
    }
  end

  defp retry_stats(meta_entries) do
    %{
      total: Map.get(meta_entries, {:runtime, :retries, :total}, 0),
      rebalance: Map.get(meta_entries, {:runtime, :retries, :rebalance}, 0),
      transport: Map.get(meta_entries, {:runtime, :retries, :transport}, 0),
      circuit_open: Map.get(meta_entries, {:runtime, :retries, :circuit_open}, 0)
    }
  end

  defp connection_stats(meta_entries, nil) do
    successful = cluster_counter(meta_entries, :connections_successful)
    closed = cluster_counter(meta_entries, :connections_closed)

    %{
      attempts: cluster_counter(meta_entries, :connections_attempts),
      successful: successful,
      failed: cluster_counter(meta_entries, :connections_failed),
      closed: closed,
      open: max(successful - closed, 0),
      idle_dropped: cluster_counter(meta_entries, :connections_idle_dropped),
      dead_dropped: cluster_counter(meta_entries, :connections_dead_dropped),
      pool_timeouts: cluster_counter(meta_entries, :connections_pool_timeouts),
      network_errors: cluster_counter(meta_entries, :connections_network_errors)
    }
  end

  defp connection_stats(meta_entries, node_name) do
    successful = Map.get(meta_entries, {:runtime, :node, node_name, :connections_successful}, 0)
    closed = Map.get(meta_entries, {:runtime, :node, node_name, :connections_closed}, 0)

    %{
      attempts: Map.get(meta_entries, {:runtime, :node, node_name, :connections_attempts}, 0),
      successful: successful,
      failed: Map.get(meta_entries, {:runtime, :node, node_name, :connections_failed}, 0),
      closed: closed,
      open: max(successful - closed, 0),
      idle_dropped:
        Map.get(meta_entries, {:runtime, :node, node_name, :connections_idle_dropped}, 0),
      dead_dropped:
        Map.get(meta_entries, {:runtime, :node, node_name, :connections_dead_dropped}, 0),
      pool_timeouts:
        Map.get(meta_entries, {:runtime, :node, node_name, :connections_pool_timeouts}, 0),
      network_errors:
        Map.get(meta_entries, {:runtime, :node, node_name, :connections_network_errors}, 0)
    }
  end

  defp lookup_config(meta_entries, key) do
    Map.get(meta_entries, {:runtime, :config, key})
  end

  defp maybe_put_config(_table, _key, nil), do: :ok

  defp maybe_put_config(table, key, value) do
    :ets.insert(table, {{:runtime, :config, key}, value})
    :ok
  end

  defp cluster_counter(meta_entries, metric) do
    Map.get(meta_entries, {:runtime, :cluster, metric}, 0)
  end

  defp bump_connection(table, nil, metric) do
    bump_counter(table, {:runtime, :cluster, connection_metric_key(metric)})
  end

  defp bump_connection(table, node_name, metric) do
    key = connection_metric_key(metric)
    bump_counter(table, {:runtime, :cluster, key})
    bump_counter(table, {:runtime, :node, node_name, key})
  end

  defp connection_metric_key(:attempts), do: :connections_attempts
  defp connection_metric_key(:successful), do: :connections_successful
  defp connection_metric_key(:failed), do: :connections_failed
  defp connection_metric_key(:closed), do: :connections_closed
  defp connection_metric_key(:idle_dropped), do: :connections_idle_dropped
  defp connection_metric_key(:dead_dropped), do: :connections_dead_dropped
  defp connection_metric_key(:pool_timeouts), do: :connections_pool_timeouts
  defp connection_metric_key(:network_errors), do: :connections_network_errors

  defp bump_counter(table, key, increment \\ 1) do
    :ets.update_counter(table, key, increment, {key, 0})
    :ok
  end

  defp command_result_tag(:ok), do: :ok
  defp command_result_tag({:ok, _value}), do: :ok
  defp command_result_tag({:error, %Error{code: code}}), do: {:error, code}
  defp command_result_tag({:error, code}) when is_atom(code), do: {:error, code}
  defp command_result_tag(%Error{code: code}), do: {:error, code}
  defp command_result_tag(_other), do: :unknown

  defp default_stats do
    %{
      metrics_enabled: false,
      cluster_ready: false,
      nodes_total: 0,
      nodes_active: 0,
      open_connections: 0,
      commands_total: 0,
      commands_ok: 0,
      commands_error: 0,
      cluster: %{
        config: %{pool_size: nil, tend_interval_ms: nil},
        tends: %{total: 0, successful: 0, failed: 0},
        nodes: %{added: 0, removed: 0},
        partition_map_updates: 0,
        retries: %{total: 0, rebalance: 0, transport: 0, circuit_open: 0},
        connections: %{
          attempts: 0,
          successful: 0,
          failed: 0,
          closed: 0,
          open: 0,
          idle_dropped: 0,
          dead_dropped: 0,
          pool_timeouts: 0,
          network_errors: 0
        },
        commands: %{
          total: 0,
          ok: 0,
          error: 0,
          latency_us_total: 0,
          by_command: %{},
          errors_by_code: %{}
        }
      },
      nodes: %{}
    }
  end
end
