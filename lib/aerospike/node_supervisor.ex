defmodule Aerospike.NodeSupervisor do
  @moduledoc """
  `DynamicSupervisor` that owns the per-node `NimblePool` children for one
  named cluster.

  This module is a shell in Tier 1: it accepts `start_pool/2` and
  `stop_pool/2` calls from the Tender but makes no autonomous decisions
  about cluster membership. The Tender decides when a pool starts or
  stops; this supervisor just holds the pool children.

  Pool children are started with `restart: :temporary` so a dead pool is
  not automatically resurrected. The Tender re-adds the node (and starts
  a fresh pool) on the next tend cycle when the node is reachable again.
  """

  @default_pool_size 10

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
  Starts the per-cluster `DynamicSupervisor`.

  Options:

    * `:name` — cluster name atom. The supervisor is registered under
      `sup_name/1`.
  """
  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts) when is_list(opts) do
    name = Keyword.fetch!(opts, :name)

    is_atom(name) or
      raise ArgumentError, "Aerospike.NodeSupervisor: :name must be an atom"

    DynamicSupervisor.start_link(strategy: :one_for_one, name: sup_name(name))
  end

  @doc """
  Returns the registered name atom used for the cluster's NodeSupervisor.
  """
  @spec sup_name(atom()) :: atom()
  def sup_name(name) when is_atom(name), do: :"#{name}_node_sup"

  @doc """
  Starts an `Aerospike.NodePool` child under this supervisor.

  Options:

    * `:node_name` — string identifier of the Aerospike node (required).
    * `:transport` — module implementing `Aerospike.NodeTransport`
      (required).
    * `:host` — host the pool workers connect to (required).
    * `:port` — TCP port the pool workers connect to (required).
    * `:connect_opts` — keyword list passed as the third argument to
      `transport.connect/3` (required).
    * `:pool_size` — positive integer, defaults to `#{@default_pool_size}`.
  """
  @spec start_pool(pid() | atom(), keyword()) :: DynamicSupervisor.on_start_child()
  def start_pool(supervisor, opts) when is_list(opts) do
    node_name = Keyword.fetch!(opts, :node_name)
    transport = Keyword.fetch!(opts, :transport)
    host = Keyword.fetch!(opts, :host)
    port = Keyword.fetch!(opts, :port)
    connect_opts = Keyword.fetch!(opts, :connect_opts)
    pool_size = Keyword.get(opts, :pool_size, @default_pool_size)

    child = %{
      id: {:node_pool, node_name},
      start:
        {NimblePool, :start_link,
         [
           [
             worker:
               {Aerospike.NodePool,
                transport: transport,
                host: host,
                port: port,
                connect_opts: connect_opts,
                node_name: node_name},
             pool_size: pool_size
           ]
         ]},
      restart: :temporary,
      shutdown: 5_000
    }

    case sup_pid(supervisor) do
      nil -> {:error, :not_found}
      sup -> DynamicSupervisor.start_child(sup, child)
    end
  end

  @doc """
  Terminates the pool child identified by `pool_pid`.

  Returns `:ok` on success, `{:error, :not_found}` when the pid is not a
  child of this supervisor (including when the supervisor itself is not
  reachable by name).
  """
  @spec stop_pool(pid() | atom(), pid()) :: :ok | {:error, :not_found}
  def stop_pool(supervisor, pool_pid) when is_pid(pool_pid) do
    case sup_pid(supervisor) do
      nil ->
        {:error, :not_found}

      sup ->
        case DynamicSupervisor.terminate_child(sup, pool_pid) do
          :ok -> :ok
          {:error, _} -> {:error, :not_found}
        end
    end
  end

  defp sup_pid(supervisor) when is_atom(supervisor), do: Process.whereis(supervisor)
  defp sup_pid(pid) when is_pid(pid), do: pid
end
