Code.require_file("../../bench_helper.exs", __DIR__)

alias Aerospike.Bench.Support.Fixtures
alias Aerospike.Bench.Support.Runtime
alias Aerospike.Protocol.AsmMsg
alias Aerospike.Protocol.Response

config = Aerospike.Bench.load_config()
metadata = Aerospike.Bench.run_metadata(config)
inputs = Fixtures.response_decode_inputs()

Runtime.print_metadata(metadata, %{workload: :mb_007})

Benchee.run(
  %{
    "MB-007 Response Decode" => fn %{body: body, key: key} ->
      {:ok, msg} = AsmMsg.decode(body)
      {:ok, _record} = Response.parse_record_response(msg, key)
    end
  },
  Aerospike.Bench.benchee_options(
    config,
    title: "L1 response decode microbenchmark",
    inputs: inputs,
    print: [benchmarking: false, configuration: false, fast_warning: false]
  )
)
