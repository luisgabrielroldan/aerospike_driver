defmodule Aerospike.NodeSupervisor do
  @moduledoc false
  # DynamicSupervisor that manages one NimblePool per discovered Aerospike node.
  # Pools are started with `:temporary` restart so a dead pool doesn't endlessly
  # retry a node that may have left the cluster — the Cluster tend loop handles
  # re-adding nodes when they come back.

  @doc false
  def child_spec(opts) when is_list(opts) do
    name = Keyword.fetch!(opts, :name)

    %{
      id: {__MODULE__, name},
      start: {__MODULE__, :start_link, [opts]},
      type: :supervisor,
      restart: :permanent,
      shutdown: :infinity
    }
  end

  @doc """
  Starts a `DynamicSupervisor` for per-node `NimblePool` children.
  """
  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts) when is_list(opts) do
    name = Keyword.fetch!(opts, :name)
    DynamicSupervisor.start_link(strategy: :one_for_one, name: sup_name(name))
  end

  @doc false
  def sup_name(name) when is_atom(name), do: :"#{name}_node_sup"

  @doc """
  Starts a `NimblePool` for the given node under this supervisor.
  """
  @spec start_pool(pid() | atom(), keyword()) :: DynamicSupervisor.on_start_child()
  def start_pool(supervisor, opts) when is_list(opts) do
    conn_name = Keyword.get(opts, :conn_name)
    pool_size = Keyword.fetch!(opts, :pool_size)
    connect_opts = Keyword.fetch!(opts, :connect_opts)
    auth_opts = Keyword.get(opts, :auth_opts, [])
    node_name = Keyword.fetch!(opts, :node_name)

    child = %{
      id: {:node_pool, node_name},
      start:
        {NimblePool, :start_link,
         [
           [
             worker:
               {Aerospike.NodePool,
                connect_opts: connect_opts,
                auth_opts: auth_opts,
                conn_name: conn_name,
                node_name: node_name},
             pool_size: pool_size
           ]
         ]},
      restart: :temporary,
      shutdown: 5_000
    }

    DynamicSupervisor.start_child(supervisor, child)
  end

  @doc """
  Stops the pool child for `pool_pid` if present.
  """
  @spec stop_pool(pid() | atom(), pid()) :: :ok | {:error, :not_found}
  def stop_pool(supervisor, pool_pid) when is_pid(pool_pid) do
    sup = sup_pid(supervisor)

    with sup when sup != nil <- sup,
         :ok <- DynamicSupervisor.terminate_child(sup, pool_pid) do
      :ok
    else
      nil -> {:error, :not_found}
      {:error, _} -> {:error, :not_found}
    end
  end

  # Accepts either a registered name or a PID so callers can use either form.
  defp sup_pid(supervisor) when is_atom(supervisor), do: Process.whereis(supervisor)
  defp sup_pid(pid) when is_pid(pid), do: pid
end
