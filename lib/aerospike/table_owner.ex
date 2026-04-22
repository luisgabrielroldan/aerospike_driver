defmodule Aerospike.TableOwner do
  @moduledoc """
  Dedicated process that creates and holds the ETS tables backing one
  named cluster's state.

  By separating ETS ownership from the Tender, a Tender crash (and
  restart under `rest_for_one`) does not drop the partition map. The
  TableOwner outlives the Tender within the same supervision subtree;
  the restarted Tender reads table names from the TableOwner and resumes
  writing to the same tables.

  The TableOwner creates four named tables with a `name`-derived prefix:

    * `owners` — partition ownership, keyed by `{namespace, partition_id}`.
    * `node_gens` — per-node `partition-generation` values.
    * `meta` — lock-free published cluster state (`:ready`,
      `:retry_opts`, `:active_nodes`).
    * `txn_tracking` - transaction state for explicit multi-record transactions.

  After `init/1`, the TableOwner has no state beyond the `:name` and the
  four table names, and handles no messages. Its sole purpose is to own
  the tables so they share its lifetime.
  """

  use GenServer

  alias Aerospike.PartitionMap
  alias Aerospike.RetryPolicy

  @type tables :: %{owners: atom(), node_gens: atom(), meta: atom(), txn_tracking: atom()}

  @type option :: {:name, atom()}

  @doc false
  def child_spec(opts) do
    name = Keyword.fetch!(opts, :name)

    %{
      id: {__MODULE__, name},
      start: {__MODULE__, :start_link, [opts]},
      type: :worker,
      restart: :permanent,
      shutdown: 5_000
    }
  end

  @doc """
  Starts the TableOwner.

  Options:

    * `:name` — cluster name atom. Used as a table-name prefix and as
      the registered name (`#{inspect(__MODULE__)}.via(name)`).
  """
  @spec start_link([option()]) :: GenServer.on_start()
  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    GenServer.start_link(__MODULE__, opts, name: via(name))
  end

  @doc """
  Returns the registered name atom used by `start_link/1` for `name`.
  """
  @spec via(atom()) :: atom()
  def via(name) when is_atom(name), do: :"#{name}_table_owner"

  @doc """
  Returns the ETS table names owned by this TableOwner.
  """
  @spec tables(GenServer.server()) :: tables()
  def tables(server) do
    GenServer.call(server, :tables)
  end

  @impl GenServer
  def init(opts) do
    name = Keyword.fetch!(opts, :name)

    is_atom(name) or raise ArgumentError, "Aerospike.TableOwner: :name must be an atom"

    {owners, node_gens} = PartitionMap.create_tables(name)
    txn_tracking = :"#{name}_txn_tracking"
    meta = :"#{name}_meta"
    :ets.new(txn_tracking, [:set, :public, :named_table, read_concurrency: true])
    :ets.new(meta, [:set, :public, :named_table, read_concurrency: true])
    :ets.insert(meta, {:ready, false})
    RetryPolicy.put(meta, RetryPolicy.defaults())
    :ets.insert(meta, {:active_nodes, []})

    {:ok,
     %{
       name: name,
       tables: %{owners: owners, node_gens: node_gens, meta: meta, txn_tracking: txn_tracking}
     }}
  end

  @impl GenServer
  def handle_call(:tables, _from, state) do
    {:reply, state.tables, state}
  end
end
