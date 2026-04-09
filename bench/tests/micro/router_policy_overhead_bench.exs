Code.require_file("../../bench_helper.exs", __DIR__)

alias Aerospike.Bench.Support.Runtime
alias Aerospike.Policy
alias Aerospike.Router
alias Aerospike.Tables

config = Aerospike.Bench.load_config()
metadata = Aerospike.Bench.run_metadata(config)
conn_name = :bench_mb_008_009
namespace = "bench"
replica_index = 0

for table <- [Tables.nodes(conn_name), Tables.partitions(conn_name), Tables.meta(conn_name)] do
  case :ets.whereis(table) do
    :undefined -> :ok
    _tid -> :ets.delete(table)
  end

  :ets.new(table, [:set, :public, :named_table, read_concurrency: true])
end

pool_pid = spawn(fn -> Process.sleep(:infinity) end)
node_name = "bench-node-a"

:ets.insert(Tables.nodes(conn_name), {node_name, %{pool_pid: pool_pid, active: true}})

for partition_id <- 0..4095 do
  :ets.insert(Tables.partitions(conn_name), {{namespace, partition_id, replica_index}, node_name})
end

default_read_opts = [timeout: 1_000, pool_checkout_timeout: 5_000, replica: :master]

default_write_opts = [
  ttl: 86_400,
  timeout: 1_000,
  generation: 3,
  gen_policy: :expect_gen_equal,
  exists: :create_or_replace,
  durable_delete: false,
  replica: :master
]

:ets.insert(Tables.meta(conn_name), {:policy_defaults, read: default_read_opts, write: default_write_opts})

inputs = %{
  "routing/256-partitions" => %{partition_id: 255, kind: :read, opts: []},
  "routing/4096-partitions" => %{partition_id: 4095, kind: :read, opts: []},
  "policy/read-override" => %{partition_id: 0, kind: :read, opts: [timeout: 500, replica: :sequence]},
  "policy/write-override" => %{
    partition_id: 0,
    kind: :write,
    opts: [ttl: 7_200, gen_policy: :expect_gen_gt]
  }
}

Runtime.print_metadata(metadata, %{workload: :mb_008_mb_009})

Benchee.run(
  %{
    "MB-008 Router Partition Resolve" => fn %{partition_id: partition_id} ->
      {:ok, _resolved_pool, _resolved_node} =
        Router.resolve_pool_for_partition(conn_name, namespace, partition_id, replica_index)
    end,
    "MB-009 Policy Merge Overhead" => fn %{kind: kind, opts: opts} ->
      Policy.merge_defaults(conn_name, kind, opts)
    end
  },
  Aerospike.Bench.benchee_options(
    config,
    title: "L1 router/policy overhead microbenchmark",
    inputs: inputs,
    print: [benchmarking: false, configuration: false, fast_warning: false]
  )
)
