defmodule Aerospike.TableOwner do
  @moduledoc false
  # GenServer whose sole purpose is to own the ETS tables for one named connection.
  # By isolating table ownership in a dedicated process, the tables survive Cluster
  # restarts — the Cluster can re-tend and repopulate without losing the table handles.

  use GenServer

  @doc false
  def child_spec(opts) when is_list(opts) do
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
  Starts the process that owns cluster ETS tables.

  Options:

  * `:name` — connection name atom (e.g. `:aero`), used for table names `name_nodes`, etc.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) when is_list(opts) do
    name = Keyword.fetch!(opts, :name)
    GenServer.start_link(__MODULE__, opts, name: via(name))
  end

  @doc false
  def via(name) when is_atom(name), do: :"#{name}_table_owner"

  @impl true
  def init(opts) do
    name = Keyword.fetch!(opts, :name)

    unless is_atom(name) do
      raise ArgumentError, ":name must be an atom"
    end

    _ =
      :ets.new(Aerospike.Tables.nodes(name), [:set, :public, :named_table, read_concurrency: true])

    _ =
      :ets.new(Aerospike.Tables.partitions(name), [
        :set,
        :public,
        :named_table,
        read_concurrency: true
      ])

    _ = :ets.new(Aerospike.Tables.txn_tracking(name), [:set, :public, :named_table])
    _ = :ets.new(Aerospike.Tables.meta(name), [:set, :public, :named_table])

    {:ok, %{name: name}}
  end
end
