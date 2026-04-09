Code.require_file("../../bench_helper.exs", __DIR__)

import Aerospike.Op

alias Aerospike.Batch
alias Aerospike.Bench.Support.Runtime
alias Aerospike.Key
alias Aerospike.Protocol.BatchEncoder

config = Aerospike.Bench.load_config()
metadata = Aerospike.Bench.run_metadata(config)

indexed_keys_100 =
  Enum.map(0..99, fn index ->
    {index, Key.new("bench", "micro", index)}
  end)

indexed_keys_1000 =
  Enum.map(0..999, fn index ->
    {index, Key.new("bench", "micro", index)}
  end)

indexed_ops_100 =
  Enum.map(indexed_keys_100, fn {index, key} ->
    {index, Batch.operate(key, [put("counter", index), get("counter")])}
  end)

indexed_ops_1000 =
  Enum.map(indexed_keys_1000, fn {index, key} ->
    {index, Batch.operate(key, [put("counter", index), get("counter")])}
  end)

Runtime.print_metadata(metadata, %{workload: :mb_008})

Benchee.run(
  %{
    "batch_exists 100" => fn ->
      BatchEncoder.encode_batch_exists(indexed_keys_100, [])
    end,
    "batch_exists 1000" => fn ->
      BatchEncoder.encode_batch_exists(indexed_keys_1000, [])
    end,
    "batch_operate_mixed 100" => fn ->
      BatchEncoder.encode_batch_operate(indexed_ops_100, [])
    end,
    "batch_operate_mixed 1000" => fn ->
      BatchEncoder.encode_batch_operate(indexed_ops_1000, [])
    end
  },
  Aerospike.Bench.benchee_options(
    config,
    title: "L1 batch encode microbenchmark",
    print: [benchmarking: false, configuration: false, fast_warning: false]
  )
)
