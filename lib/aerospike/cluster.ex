defmodule Aerospike.Cluster do
  @moduledoc """
  Thin read-side seam over published cluster state.

  `ready?/1`, routing, retry policy, and active-node helpers read the
  cluster's ETS tables directly when the caller passes the cluster atom
  or a pid registered under that atom. Operational APIs such as
  transports, pool checkout, and node handles remain on
  `Aerospike.Cluster.Tender`.
  """

  alias Aerospike.Key
  alias Aerospike.Cluster.Router
  alias Aerospike.RetryPolicy

  @type cluster :: GenServer.server()
  @type tables :: Router.tables()
  @type replica_policy :: Router.replica_policy()
  @type route_result :: {:ok, String.t()} | {:error, :cluster_not_ready | :no_master}

  @doc """
  Returns the published ETS tables for `cluster`.
  """
  @spec tables(cluster()) :: tables()
  def tables(cluster)

  def tables(cluster) when is_atom(cluster) do
    %{
      owners: :"#{cluster}_partition_map_owners",
      node_gens: :"#{cluster}_partition_map_node_gens",
      meta: :"#{cluster}_meta",
      txn_tracking: :"#{cluster}_txn_tracking"
    }
  end

  def tables(cluster) when is_pid(cluster), do: cluster |> registered_name!() |> tables()

  def tables(_cluster),
    do: raise(ArgumentError, "cluster identity must be an atom or a pid registered under one")

  @doc """
  Returns whether every configured namespace has a complete partition map.
  """
  @spec ready?(cluster()) :: boolean()
  def ready?(cluster) do
    cluster
    |> tables()
    |> Map.fetch!(:meta)
    |> read_meta(:ready, false)
  end

  @doc """
  Returns the cluster-default retry policy published in `meta.:retry_opts`.
  """
  @spec retry_policy(cluster()) :: RetryPolicy.t()
  def retry_policy(cluster) do
    cluster
    |> tables()
    |> Map.fetch!(:meta)
    |> load_retry_policy()
  end

  @doc """
  Returns the published active node-name snapshot.
  """
  @spec active_nodes(cluster()) :: [String.t()]
  def active_nodes(cluster) do
    cluster
    |> tables()
    |> Map.fetch!(:meta)
    |> read_meta(:active_nodes, [])
  end

  @doc """
  Returns whether `node_name` appears in the published active-node snapshot.
  """
  @spec active_node?(cluster(), String.t()) :: boolean()
  def active_node?(cluster, node_name) when is_binary(node_name) do
    node_name in active_nodes(cluster)
  end

  @doc """
  Routes a read for `key` under `policy` and `attempt`.
  """
  @spec route_for_read(cluster(), Key.t(), replica_policy(), non_neg_integer()) :: route_result()
  def route_for_read(cluster, %Key{} = key, policy, attempt)
      when policy in [:master, :sequence] and is_integer(attempt) and attempt >= 0 do
    Router.pick_for_read(tables(cluster), key, policy, attempt)
  end

  @doc """
  Routes a write for `key` to its master node.
  """
  @spec route_for_write(cluster(), Key.t()) :: route_result()
  def route_for_write(cluster, %Key{} = key) do
    Router.pick_for_write(tables(cluster), key)
  end

  defp registered_name!(cluster) when is_pid(cluster) do
    case Process.info(cluster, :registered_name) do
      {:registered_name, name} when is_atom(name) -> name
      _ -> raise ArgumentError, "cluster identity must be an atom or a pid registered under one"
    end
  end

  defp read_meta(meta_tab, key, default) do
    case :ets.lookup(meta_tab, key) do
      [{^key, value}] -> value
      _ -> default
    end
  catch
    :error, :badarg -> default
  end

  defp load_retry_policy(meta_tab) do
    RetryPolicy.load(meta_tab)
  catch
    :error, :badarg -> RetryPolicy.defaults()
  end
end
