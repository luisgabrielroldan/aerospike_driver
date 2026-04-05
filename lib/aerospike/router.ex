defmodule Aerospike.Router do
  @moduledoc false
  # Routes a wire-encoded request to the correct node based on the key's partition.
  #
  # Flow: key → partition_id → ETS partition table → node_name → ETS nodes table
  # → pool_pid → NimblePool.checkout! → send wire + receive response.

  alias Aerospike.Connection
  alias Aerospike.Error
  alias Aerospike.Key
  alias Aerospike.Tables

  @doc """
  Resolves routing for `key`, checks out a pooled connection, sends `wire`, and returns the AS_MSG body.

  Returns `{:ok, body, node_name}` on success or `{:error, %Aerospike.Error{}}`.

  `node_name` is the Aerospike cluster node string (e.g. from the `node` info field).
  """
  @spec run(atom(), Key.t(), iodata(), keyword()) ::
          {:ok, binary(), String.t()} | {:error, Error.t()}
  def run(conn_name, %Key{} = key, wire, opts \\ []) when is_atom(conn_name) do
    checkout_timeout = Keyword.get(opts, :pool_checkout_timeout, 5_000)
    replica_index = Keyword.get(opts, :replica_index, 0)

    with :ok <- check_ready(conn_name),
         {:ok, pool_pid, node_name} <- resolve_pool(conn_name, key, replica_index),
         {:ok, body} <- checkout_with_wire(pool_pid, wire, checkout_timeout) do
      {:ok, body, node_name}
    else
      {:error, %Error{} = e} ->
        {:error, e}

      {:error, other} ->
        {:error, Error.from_result_code(:parse_error, message: inspect(other))}
    end
  end

  # Guards against requests before the cluster has finished its initial tend.
  defp check_ready(name) do
    case :ets.lookup(Tables.meta(name), Tables.ready_key()) do
      [{_, true}] -> :ok
      _ -> {:error, Error.from_result_code(:cluster_not_ready)}
    end
  end

  # Looks up which node owns {namespace, partition_id, replica_index} in the
  # partitions ETS table, then finds that node's pool PID.
  defp resolve_pool(name, key, replica_index) do
    partition_id = Key.partition_id(key)
    ns = key.namespace

    case :ets.lookup(Tables.partitions(name), {ns, partition_id, replica_index}) do
      [] ->
        {:error, Error.from_result_code(:invalid_cluster_partition_map)}

      [{_, node_name}] ->
        lookup_pool(name, node_name)
    end
  end

  defp lookup_pool(name, node_name) do
    case :ets.lookup(Tables.nodes(name), node_name) do
      [] ->
        {:error, Error.from_result_code(:invalid_node)}

      [{_, %{pool_pid: pid}}] when is_pid(pid) ->
        {:ok, pid, node_name}

      _ ->
        {:error, Error.from_result_code(:invalid_node)}
    end
  end

  # Checks out a connection from the pool, sends the wire bytes, and reads
  # the response. Returns `:close` as the checkin value on error so the pool
  # discards the broken connection.
  defp checkout_with_wire(pool_pid, wire, checkout_timeout) do
    NimblePool.checkout!(
      pool_pid,
      :checkout,
      fn _from, conn ->
        case Connection.request(conn, wire) do
          {:ok, conn2, _v, _t, body} ->
            {{:ok, body}, conn2}

          {:error, reason} ->
            e = Error.from_result_code(:network_error, message: inspect(reason))
            {{:error, e}, :close}
        end
      end,
      checkout_timeout
    )
  catch
    :exit, {:timeout, {NimblePool, :checkout, _}} ->
      {:error, Error.from_result_code(:pool_timeout)}

    :exit, {:noproc, {NimblePool, :checkout, _}} ->
      {:error, Error.from_result_code(:invalid_node)}

    :exit, reason ->
      {:error, Error.from_result_code(:network_error, message: inspect(reason))}
  end
end
