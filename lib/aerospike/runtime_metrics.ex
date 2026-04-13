defmodule Aerospike.RuntimeMetrics do
  @moduledoc false

  alias Aerospike.Error
  alias Aerospike.Tables

  @metrics_enabled_key {:runtime, :metrics_enabled}

  @spec init(atom(), keyword()) :: :ok
  def init(conn, opts \\ []) when is_atom(conn) and is_list(opts) do
    with table when table != :undefined <- :ets.whereis(Tables.meta(conn)) do
      :ets.insert_new(table, {@metrics_enabled_key, false})

      maybe_put_config(table, :pool_size, Keyword.get(opts, :pool_size))
      maybe_put_config(table, :tend_interval_ms, Keyword.get(opts, :tend_interval))
    end

    :ok
  end

  @doc false
  @spec record(atom(), atom(), integer(), term(), nil | String.t()) :: :ok
  def record(conn, command, start_mono, result, node_name)
      when is_atom(conn) and is_atom(command) and is_integer(start_mono) do
    if metrics_enabled?(conn) do
      duration_us =
        System.convert_time_unit(System.monotonic_time() - start_mono, :native, :microsecond)

      record_command_counters(conn, command, node_name, duration_us)
      record_command_result(conn, node_name, command, command_result_tag(result))
    end

    :ok
  end

  @spec metrics_enabled?(atom()) :: boolean()
  def metrics_enabled?(conn) when is_atom(conn) do
    case lookup_meta(conn, @metrics_enabled_key) do
      [{@metrics_enabled_key, enabled?}] -> enabled?
      _ -> false
    end
  end

  @spec enable(atom(), keyword()) :: :ok | {:error, Error.t()}
  def enable(conn, opts \\ []) when is_atom(conn) and is_list(opts) do
    if table_exists?(conn) do
      if Keyword.get(opts, :reset, false) do
        clear_command_metrics(conn)
      end

      :ets.insert(Tables.meta(conn), {@metrics_enabled_key, true})
      :ok
    else
      {:error, Error.from_result_code(:cluster_not_ready)}
    end
  end

  @spec disable(atom()) :: :ok | {:error, Error.t()}
  def disable(conn) when is_atom(conn) do
    if table_exists?(conn) do
      :ets.insert(Tables.meta(conn), {@metrics_enabled_key, false})
      :ok
    else
      {:error, Error.from_result_code(:cluster_not_ready)}
    end
  end

  @spec stats(atom()) :: map()
  def stats(conn) when is_atom(conn) do
    if table_exists?(conn) do
      meta_entries = Map.new(:ets.tab2list(Tables.meta(conn)))
      node_rows = current_node_rows(conn)

      commands = command_stats(meta_entries)
      cluster_connections = connection_stats(meta_entries, nil)
      nodes_total = map_size(node_rows)

      nodes_active =
        Enum.count(node_rows, fn {_node_name, row} -> Map.get(row, :active, false) end)

      %{
        metrics_enabled: metrics_enabled?(conn),
        cluster_ready: Map.get(meta_entries, Tables.ready_key(), false),
        nodes_total: nodes_total,
        nodes_active: nodes_active,
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
          connections: cluster_connections,
          commands: commands
        },
        nodes:
          Map.new(node_rows, fn {node_name, row} ->
            {node_name,
             %{
               host: Map.get(row, :host, ""),
               port: Map.get(row, :port, 0),
               active: Map.get(row, :active, false),
               connections: connection_stats(meta_entries, node_name),
               commands: command_stats(meta_entries, node_name)
             }}
          end)
      }
    else
      default_stats()
    end
  end

  @spec record_tend(atom(), :ok | :error) :: :ok
  def record_tend(conn, result) when is_atom(conn) and result in [:ok, :error] do
    bump_cluster(conn, :tends_total)

    case result do
      :ok -> bump_cluster(conn, :tends_successful)
      :error -> bump_cluster(conn, :tends_failed)
    end

    :ok
  end

  @spec record_partition_map_update(atom()) :: :ok
  def record_partition_map_update(conn) when is_atom(conn) do
    bump_cluster(conn, :partition_map_updates)
    :ok
  end

  @spec record_node_added(atom(), String.t()) :: :ok
  def record_node_added(conn, node_name) when is_atom(conn) and is_binary(node_name) do
    bump_cluster(conn, :node_added_count)
    :ok
  end

  @spec record_node_removed(atom(), String.t()) :: :ok
  def record_node_removed(conn, node_name) when is_atom(conn) and is_binary(node_name) do
    bump_cluster(conn, :node_removed_count)
    :ok
  end

  @spec record_connection_attempt(atom() | nil, String.t() | nil) :: :ok
  def record_connection_attempt(nil, _node_name), do: :ok

  def record_connection_attempt(conn, node_name) when is_atom(conn) do
    bump_connection(conn, node_name, :attempts)
    :ok
  end

  @spec record_connection_success(atom() | nil, String.t() | nil) :: :ok
  def record_connection_success(nil, _node_name), do: :ok

  def record_connection_success(conn, node_name) when is_atom(conn) do
    bump_connection(conn, node_name, :successful)
    :ok
  end

  @spec record_connection_failure(atom() | nil, String.t() | nil) :: :ok
  def record_connection_failure(nil, _node_name), do: :ok

  def record_connection_failure(conn, node_name) when is_atom(conn) do
    bump_connection(conn, node_name, :failed)
    :ok
  end

  @spec record_connection_closed(atom() | nil, String.t() | nil) :: :ok
  def record_connection_closed(nil, _node_name), do: :ok

  def record_connection_closed(conn, node_name) when is_atom(conn) do
    bump_connection(conn, node_name, :closed)
    :ok
  end

  @spec record_connection_drop(atom() | nil, String.t() | nil, :idle | :dead) :: :ok
  def record_connection_drop(nil, _node_name, _reason), do: :ok

  def record_connection_drop(conn, node_name, :idle) when is_atom(conn) do
    bump_connection(conn, node_name, :idle_dropped)
    :ok
  end

  def record_connection_drop(conn, node_name, :dead) when is_atom(conn) do
    bump_connection(conn, node_name, :dead_dropped)
    :ok
  end

  @spec record_checkout_failure(atom() | nil, String.t() | nil, :pool_timeout | :network_error) ::
          :ok
  def record_checkout_failure(nil, _node_name, _reason), do: :ok

  def record_checkout_failure(conn, node_name, :pool_timeout) when is_atom(conn) do
    bump_connection(conn, node_name, :pool_timeouts)
    :ok
  end

  def record_checkout_failure(conn, node_name, :network_error) when is_atom(conn) do
    bump_connection(conn, node_name, :network_errors)
    :ok
  end

  defp command_result_tag(:ok), do: :ok

  defp command_result_tag({:ok, _}), do: :ok

  defp command_result_tag({:error, %Error{code: code}}), do: {:error, code}

  defp command_result_tag({:error, code}) when is_atom(code), do: {:error, code}

  defp command_result_tag(other), do: other

  defp record_command_result(conn, node_name, command, :ok) do
    bump_counter(conn, {:runtime, :commands, :ok})
    bump_counter(conn, {:runtime, :command, command, :ok})

    if is_binary(node_name) do
      bump_counter(conn, {:runtime, :node_command, node_name, :ok})
      bump_counter(conn, {:runtime, :node_command, node_name, command, :ok})
    end
  end

  defp record_command_result(conn, node_name, command, {:error, code}) when is_atom(code) do
    bump_counter(conn, {:runtime, :commands, :error})
    bump_counter(conn, {:runtime, :command, command, :error})
    bump_counter(conn, {:runtime, :error_code, code})

    if is_binary(node_name) do
      bump_counter(conn, {:runtime, :node_command, node_name, :error})
      bump_counter(conn, {:runtime, :node_command, node_name, command, :error})
      bump_counter(conn, {:runtime, :node_error_code, node_name, code})
    end
  end

  defp record_command_result(conn, node_name, command, other) do
    if other == :ok do
      record_command_result(conn, node_name, command, :ok)
    else
      record_command_result(conn, node_name, command, {:error, :unknown})
    end
  end

  defp record_command_counters(conn, command, node_name, duration_us) do
    bump_counter(conn, {:runtime, :commands, :total})
    bump_counter(conn, {:runtime, :commands, :latency_us_total}, duration_us)
    bump_counter(conn, {:runtime, :command, command, :total})
    bump_counter(conn, {:runtime, :command, command, :latency_us_total}, duration_us)
    record_node_command_counters(conn, node_name, command, duration_us)
  end

  defp record_node_command_counters(_conn, node_name, _command, _duration_us)
       when not is_binary(node_name),
       do: :ok

  defp record_node_command_counters(conn, node_name, command, duration_us) do
    bump_counter(conn, {:runtime, :node_command, node_name, :total})
    bump_counter(conn, {:runtime, :node_command, node_name, :latency_us_total}, duration_us)
    bump_counter(conn, {:runtime, :node_command, node_name, command, :total})

    bump_counter(
      conn,
      {:runtime, :node_command, node_name, command, :latency_us_total},
      duration_us
    )
  end

  defp command_stats(meta_entries, node_name \\ nil) do
    by_command =
      Enum.reduce(meta_entries, %{}, fn
        {{:runtime, :command, command, metric}, value}, acc
        when is_nil(node_name) and is_atom(command) and is_atom(metric) ->
          Map.update(acc, command, %{metric => value}, &Map.put(&1, metric, value))

        {{:runtime, :node_command, ^node_name, command, metric}, value}, acc
        when is_binary(node_name) and is_atom(command) and is_atom(metric) ->
          Map.update(acc, command, %{metric => value}, &Map.put(&1, metric, value))

        _, acc ->
          acc
      end)

    errors_by_code =
      Enum.reduce(meta_entries, %{}, fn
        {{:runtime, :node_error_code, ^node_name, code}, value}, acc ->
          Map.put(acc, code, value)

        {{:runtime, :error_code, code}, value}, acc when is_nil(node_name) ->
          Map.put(acc, code, value)

        _, acc ->
          acc
      end)

    if is_binary(node_name) do
      %{
        total: Map.get(meta_entries, {:runtime, :node_command, node_name, :total}, 0),
        ok: Map.get(meta_entries, {:runtime, :node_command, node_name, :ok}, 0),
        error: Map.get(meta_entries, {:runtime, :node_command, node_name, :error}, 0),
        latency_us_total:
          Map.get(meta_entries, {:runtime, :node_command, node_name, :latency_us_total}, 0),
        by_command: by_command,
        errors_by_code: errors_by_code
      }
    else
      %{
        total: Map.get(meta_entries, {:runtime, :commands, :total}, 0),
        ok: Map.get(meta_entries, {:runtime, :commands, :ok}, 0),
        error: Map.get(meta_entries, {:runtime, :commands, :error}, 0),
        latency_us_total: Map.get(meta_entries, {:runtime, :commands, :latency_us_total}, 0),
        by_command: by_command,
        errors_by_code: errors_by_code
      }
    end
  end

  defp connection_stats(meta_entries, node_name) do
    if is_binary(node_name) do
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
    else
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
  end

  defp cluster_counter(meta_entries, metric) do
    Map.get(meta_entries, {:runtime, :cluster, metric}, 0)
  end

  defp current_node_rows(conn) do
    nodes_table = Tables.nodes(conn)

    if :ets.whereis(nodes_table) == :undefined do
      %{}
    else
      Map.new(:ets.tab2list(nodes_table))
    end
  end

  defp lookup_meta(conn, key) do
    table = Tables.meta(conn)

    if :ets.whereis(table) == :undefined do
      []
    else
      :ets.lookup(table, key)
    end
  end

  defp lookup_config(meta_entries, key) do
    Map.get(meta_entries, {:runtime, :config, key})
  end

  defp maybe_put_config(_table, _key, nil), do: :ok

  defp maybe_put_config(table, key, value) do
    :ets.insert(table, {{:runtime, :config, key}, value})
    :ok
  end

  defp bump_cluster(conn, metric) do
    bump_counter(conn, {:runtime, :cluster, metric})
  end

  defp bump_connection(conn, nil, metric) do
    bump_counter(conn, {:runtime, :cluster, connection_metric_key(metric)})
  end

  defp bump_connection(conn, node_name, metric) when is_binary(node_name) do
    key = connection_metric_key(metric)
    bump_counter(conn, {:runtime, :cluster, key})
    bump_counter(conn, {:runtime, :node, node_name, key})
  end

  defp connection_metric_key(:attempts), do: :connections_attempts
  defp connection_metric_key(:successful), do: :connections_successful
  defp connection_metric_key(:failed), do: :connections_failed
  defp connection_metric_key(:closed), do: :connections_closed
  defp connection_metric_key(:idle_dropped), do: :connections_idle_dropped
  defp connection_metric_key(:dead_dropped), do: :connections_dead_dropped
  defp connection_metric_key(:pool_timeouts), do: :connections_pool_timeouts
  defp connection_metric_key(:network_errors), do: :connections_network_errors

  defp bump_counter(conn, key, amount \\ 1)

  defp bump_counter(conn, _key, 0) when is_atom(conn), do: :ok

  defp bump_counter(conn, key, amount) when is_atom(conn) and is_integer(amount) do
    table = Tables.meta(conn)

    if :ets.whereis(table) != :undefined do
      _ = :ets.update_counter(table, key, {2, amount}, {key, 0})
    end

    :ok
  end

  defp clear_command_metrics(conn) do
    table = Tables.meta(conn)

    if :ets.whereis(table) != :undefined do
      Enum.each(:ets.tab2list(table), &maybe_clear_command_metric(table, &1))
    end

    :ok
  end

  defp maybe_clear_command_metric(table, {key, _value}) when is_tuple(key) do
    if clearable_command_metric_key?(key) do
      :ets.delete(table, key)
    end

    :ok
  end

  defp maybe_clear_command_metric(_table, _entry), do: :ok

  defp clearable_command_metric_key?({:runtime, :commands, _metric}), do: true
  defp clearable_command_metric_key?({:runtime, :command, _command, _metric}), do: true
  defp clearable_command_metric_key?({:runtime, :error_code, _code}), do: true
  defp clearable_command_metric_key?({:runtime, :node_command, _node_name, _metric}), do: true

  defp clearable_command_metric_key?({:runtime, :node_command, _node_name, _command, _metric}),
    do: true

  defp clearable_command_metric_key?({:runtime, :node_error_code, _node_name, _code}), do: true
  defp clearable_command_metric_key?(_), do: false

  defp table_exists?(conn) do
    :ets.whereis(Tables.meta(conn)) != :undefined
  end

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
