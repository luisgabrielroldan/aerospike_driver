Code.require_file("../../bench_helper.exs", __DIR__)

alias Aerospike.Bench.Support.Runtime
alias Aerospike.Protocol.AsmMsg.Value

config = Aerospike.Bench.load_config()
metadata = Aerospike.Bench.run_metadata(config)
inputs = Aerospike.Bench.primitive_encoding_inputs()

Runtime.print_metadata(metadata, %{workload: :mb_003})

jobs =
  Enum.into(inputs, %{}, fn {label, value} ->
    {"MB-003 Primitive Encoding #{label}", fn -> Value.encode_value(value) end}
  end)

Benchee.run(
  jobs,
  Aerospike.Bench.benchee_options(
    config,
    title: "L1 primitive value encoding microbenchmark",
    print: [benchmarking: false, configuration: false, fast_warning: false]
  )
)
