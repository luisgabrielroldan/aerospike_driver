Code.require_file("../../bench_helper.exs", __DIR__)

defmodule Aerospike.Bench.Micro.BatchRouting do
  @moduledoc false

  alias Aerospike.Bench.Support.Runtime
  alias Aerospike.Cluster.PartitionMap
  alias Aerospike.Cluster.Router
  alias Aerospike.Command.BatchCommand.Entry
  alias Aerospike.Command.BatchRouter
  alias Aerospike.Key

  @namespace "bench"
  @set "batch_routing"
  @batch_sizes [100, 1_000]
  @iterations 32
  @node_counts [1, 3, 8]

  def run do
    config = Aerospike.Bench.load_config()
    metadata = Aerospike.Bench.run_metadata(config, %{iterations_per_sample: @iterations})

    Runtime.print_metadata(metadata, %{workload: :batch_routing_micro})

    Benchee.run(
      jobs(),
      Aerospike.Bench.benchee_options(
        config,
        title: "L1 batch routing baseline",
        inputs: inputs(),
        print: [benchmarking: false, configuration: false, fast_warning: false]
      )
    )
  end

  defp jobs do
    %{
      "BR-001 Group read entries x#{@iterations}" => fn %{
                                                          tables: tables,
                                                          read_entries: entries
                                                        } ->
        repeat(entries, &BatchRouter.group_entries(tables, &1))
      end,
      "BR-002 Group write entries x#{@iterations}" => fn %{
                                                           tables: tables,
                                                           write_entries: entries
                                                         } ->
        repeat(entries, &BatchRouter.group_entries(tables, &1))
      end,
      "BR-003 Group read keys x#{@iterations}" => fn %{tables: tables, keys: keys} ->
        repeat(keys, &BatchRouter.group_keys(tables, &1, dispatch: {:read, :master, 0}))
      end,
      "BR-004 Route master reads x#{@iterations}" => fn %{tables: tables, keys: keys} ->
        repeat(keys, &route_keys(tables, &1, :master))
      end,
      "BR-005 Route sequence reads x#{@iterations}" => fn %{tables: tables, keys: keys} ->
        repeat(keys, &route_keys(tables, &1, :sequence))
      end
    }
  end

  defp inputs do
    for batch_size <- @batch_sizes,
        node_count <- @node_counts,
        into: %{} do
      {"#{batch_size} keys / #{node_count} nodes", build_input(batch_size, node_count)}
    end
  end

  defp build_input(batch_size, node_count) do
    tables = create_tables(batch_size)
    seed_partitions(tables, node_count)

    key_runs =
      Enum.map(1..@iterations, fn iteration ->
        keys(batch_size, iteration)
      end)

    %{
      tables: tables,
      keys: key_runs,
      read_entries: Enum.map(key_runs, &entries(&1, {:read, :master, 0}, :read)),
      write_entries: Enum.map(key_runs, &entries(&1, :write, :put))
    }
  end

  defp create_tables(batch_size) do
    prefix = :"batch_routing_bench_#{batch_size}_#{System.unique_integer([:positive])}"
    {owners, node_gens} = PartitionMap.create_tables(prefix)
    meta = :"#{prefix}_meta"
    :ets.new(meta, [:set, :public, :named_table, read_concurrency: true])
    :ets.insert(meta, {:ready, true})

    %{owners: owners, node_gens: node_gens, meta: meta}
  end

  defp seed_partitions(%{owners: owners}, node_count) do
    Enum.each(0..4095, fn partition_id ->
      master_index = rem(partition_id, node_count)

      replicas = [
        node_name(master_index, node_count),
        node_name(master_index + 1, node_count),
        node_name(master_index + 2, node_count)
      ]

      :ok = PartitionMap.update(owners, @namespace, partition_id, 1, replicas)
    end)
  end

  defp keys(batch_size, iteration) do
    Enum.map(1..batch_size, fn index ->
      Key.new(@namespace, @set, "br:#{batch_size}:#{iteration}:#{index}")
    end)
  end

  defp entries(keys, dispatch, kind) do
    keys
    |> Enum.with_index()
    |> Enum.map(fn {key, index} ->
      %Entry{
        index: index,
        key: key,
        kind: kind,
        dispatch: dispatch,
        payload: nil
      }
    end)
  end

  defp repeat(inputs, fun) do
    Enum.reduce(inputs, nil, fn input, _acc -> fun.(input) end)
  end

  defp route_keys(tables, keys, policy) do
    keys
    |> Enum.with_index()
    |> Enum.reduce(nil, fn {key, attempt}, _acc ->
      Router.pick_for_read_ready(tables, key, policy, attempt)
    end)
  end

  defp node_name(index, node_count), do: "bench-node-#{rem(index, node_count)}"
end

Aerospike.Bench.Micro.BatchRouting.run()
