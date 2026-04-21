defmodule Aerospike.ExecuteTask do
  @moduledoc """
  Tracks background query execution progress.
  """

  alias Aerospike.Error
  alias Aerospike.NodePool
  alias Aerospike.Tender

  @type kind :: :query_execute | :query_udf

  @default_checkout_timeout 5_000
  @default_poll_interval 1_000
  @max_unobserved_not_found_polls 3

  @enforce_keys [:conn, :namespace, :set, :task_id, :kind]
  defstruct [:conn, :namespace, :set, :task_id, :kind, node_name: nil]

  @type t :: %__MODULE__{
          conn: GenServer.server(),
          namespace: String.t(),
          set: String.t(),
          task_id: non_neg_integer(),
          kind: kind(),
          node_name: String.t() | nil
        }

  use Aerospike.AsyncTask

  @impl Aerospike.AsyncTask
  def status(%__MODULE__{} = task) do
    case poll_state(task, MapSet.new(), 0) do
      {:ok, status, _observed, _not_found_polls} -> {:ok, status}
      {:error, %Error{} = err} -> {:error, err}
    end
  end

  @impl Aerospike.AsyncTask
  def wait(%__MODULE__{} = task, opts) do
    poll_interval = Keyword.get(opts, :poll_interval, @default_poll_interval)
    timeout_ms = Keyword.get(opts, :timeout, nil)
    deadline = if timeout_ms, do: System.monotonic_time(:millisecond) + timeout_ms, else: nil

    do_wait(task, poll_interval, deadline, MapSet.new(), 0)
  end

  @doc false
  def parse_status_response(response) when is_binary(response) do
    cond do
      response == "" ->
        :not_found

      Regex.match?(~r/^ERROR:2(?::|$)/, response) ->
        :not_found

      String.starts_with?(response, "ERROR:") ->
        {:error, Error.from_result_code(:server_error, message: response)}

      true ->
        parse_status_value(response)
    end
  end

  defp do_wait(task, poll_interval, deadline, observed, not_found_polls) do
    case poll_state(task, observed, not_found_polls) do
      {:ok, :complete, _observed2, _not_found_polls2} ->
        :ok

      {:ok, :in_progress, observed2, not_found_polls2} ->
        if deadline && System.monotonic_time(:millisecond) >= deadline do
          {:error, Error.from_result_code(:timeout)}
        else
          Process.sleep(poll_interval)
          do_wait(task, poll_interval, deadline, observed2, not_found_polls2)
        end

      {:error, %Error{} = err} ->
        {:error, err}
    end
  end

  defp poll_state(task, observed, not_found_polls) do
    with {:ok, node_names} <- target_node_names(task) do
      evaluate_node_statuses(task, node_names, observed, not_found_polls)
    end
  end

  defp evaluate_node_statuses(task, node_names, observed, not_found_polls) do
    Enum.reduce_while(node_names, {:ok, observed, []}, fn node_name,
                                                          {:ok, observed_acc, states} ->
      case fetch_node_status(task, node_name) do
        {:ok, :in_progress} ->
          {:halt, {:ok, :in_progress, MapSet.put(observed_acc, node_name), 0}}

        {:ok, :complete} ->
          {:cont, {:ok, MapSet.put(observed_acc, node_name), [:complete | states]}}

        {:ok, :not_found} ->
          {:cont, {:ok, observed_acc, [{:not_found, node_name} | states]}}

        {:error, %Error{} = err} ->
          {:halt, {:error, ensure_error_node(err, node_name)}}
      end
    end)
    |> finalize_node_statuses(observed, not_found_polls)
  end

  defp finalize_node_statuses(
         {:ok, :in_progress, observed, not_found_polls},
         _old_observed,
         _old_nfp
       ),
       do: {:ok, :in_progress, observed, not_found_polls}

  defp finalize_node_statuses({:error, %Error{} = err}, _old_observed, _old_nfp),
    do: {:error, err}

  defp finalize_node_statuses({:ok, observed, states}, _old_observed, not_found_polls) do
    cond do
      states == [] ->
        {:ok, :complete, observed, 0}

      Enum.all?(states, &(&1 == :complete)) ->
        {:ok, :complete, observed, 0}

      true ->
        unknown_nodes = unknown_not_found_nodes(states, observed)

        if unknown_nodes == [] do
          {:ok, :complete, observed, 0}
        else
          finalize_unobserved_not_found(observed, not_found_polls)
        end
    end
  end

  defp fetch_node_status(task, node_name) do
    commands = candidate_commands(task)

    with {:ok, handle} <- Tender.node_handle(task.conn, node_name) do
      transport = Tender.transport(task.conn)

      node_name
      |> fetch_status_responses(handle.pool, transport, commands)
      |> handle_status_responses()
    end
  end

  defp fetch_status_responses(node_name, pool, transport, commands) do
    NodePool.checkout!(
      node_name,
      pool,
      fn conn ->
        {transport.info(conn, commands), conn}
      end,
      @default_checkout_timeout
    )
  end

  defp handle_status_responses({:ok, map}) when is_map(map), do: handle_status_responses(map)

  defp handle_status_responses({:error, %Error{} = err}), do: {:error, err}

  defp handle_status_responses(map) when is_map(map) do
    map
    |> Enum.map(fn {command, response} -> {command, parse_status_response(response)} end)
    |> choose_status_result()
  end

  defp choose_status_result(results) do
    case Enum.find(results, fn {_command, result} -> result in [:complete, :in_progress] end) do
      {_command, result} ->
        {:ok, result}

      nil ->
        choose_status_error(results)
    end
  end

  defp candidate_commands(%__MODULE__{task_id: task_id}) do
    task_id_str = Integer.to_string(task_id)

    [
      "query-show:id=#{task_id_str}",
      "query-show:trid=#{task_id_str}",
      "jobs:module=query;cmd=get-job;id=#{task_id_str}",
      "jobs:module=query;cmd=get-job;trid=#{task_id_str}"
    ]
  end

  defp target_node_names(%__MODULE__{conn: conn, node_name: node_name})
       when is_binary(node_name) do
    if cluster_ready?(conn) do
      case tender_nodes_status(conn) |> Map.fetch(node_name) do
        {:ok, %{status: :active}} ->
          {:ok, [node_name]}

        {:ok, %{status: :inactive}} ->
          {:error, Error.from_result_code(:invalid_node, node: node_name)}

        :error ->
          {:error, Error.from_result_code(:invalid_node, node: node_name)}
      end
    else
      {:error, Error.from_result_code(:cluster_not_ready)}
    end
  end

  defp target_node_names(%__MODULE__{conn: conn}) do
    if cluster_ready?(conn) do
      node_names =
        tender_nodes_status(conn)
        |> Enum.filter(fn {_node_name, meta} -> Map.get(meta, :status) == :active end)
        |> Enum.map(&elem(&1, 0))

      case node_names do
        [] -> {:error, Error.from_result_code(:invalid_node)}
        _ -> {:ok, node_names}
      end
    else
      {:error, Error.from_result_code(:cluster_not_ready)}
    end
  end

  defp cluster_ready?(conn) do
    tender_ready?(conn)
  end

  defp parse_status_value(response) do
    case Regex.run(~r/status=([^:;]+)/i, response) do
      [_, status] ->
        if String.starts_with?(String.downcase(status), "done") do
          :complete
        else
          :in_progress
        end

      nil ->
        :not_found
    end
  end

  defp ensure_error_node(%Error{node: nil} = err, node_name), do: %{err | node: node_name}
  defp ensure_error_node(%Error{} = err, _node_name), do: err

  defp finalize_unobserved_not_found(observed, not_found_polls) do
    next_polls = not_found_polls + 1

    if next_polls >= @max_unobserved_not_found_polls do
      {:ok, :complete, observed, next_polls}
    else
      {:ok, :in_progress, observed, next_polls}
    end
  end

  defp choose_status_error(results) do
    case Enum.find(results, fn {_command, result} -> match?({:error, _}, result) end) do
      {_command, {:error, %Error{} = err}} ->
        {:error, err}

      nil ->
        {:ok, :not_found}
    end
  end

  defp unknown_not_found_nodes(states, observed) do
    Enum.flat_map(states, &unknown_not_found_node(&1, observed))
  end

  defp unknown_not_found_node({:not_found, node_name}, observed) do
    if MapSet.member?(observed, node_name), do: [], else: [node_name]
  end

  defp unknown_not_found_node(_state, _observed), do: []

  defp tender_ready?(conn) do
    Tender.ready?(conn)
  catch
    :exit, _ -> false
  end

  defp tender_nodes_status(conn) do
    Tender.nodes_status(conn)
  catch
    :exit, _ -> %{}
  end
end
