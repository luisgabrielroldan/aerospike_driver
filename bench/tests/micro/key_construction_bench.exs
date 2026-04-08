Code.require_file("../../bench_helper.exs", __DIR__)

alias Aerospike.Bench.Support.Runtime
alias Aerospike.Key

config = Aerospike.Bench.load_config()
metadata = Aerospike.Bench.run_metadata(config)

namespace = "bench"
set = "micro"

integer_key = 42
string_16 = String.duplicate("a", 16)
string_128 = String.duplicate("b", 128)
binary_256 = :binary.copy(<<171>>, 256)

inputs = %{
  "int64/8B" => %{
    key_tuple: {namespace, set, integer_key},
    digest_payload: <<set::binary, 1::8, integer_key::signed-big-64>>
  },
  "string/16B" => %{
    key_tuple: {namespace, set, string_16},
    digest_payload: <<set::binary, 3::8, string_16::binary>>
  },
  "string/128B" => %{
    key_tuple: {namespace, set, string_128},
    digest_payload: <<set::binary, 3::8, string_128::binary>>
  },
  "binary-like/256B" => %{
    key_tuple: {namespace, set, binary_256},
    digest_payload: <<set::binary, 4::8, binary_256::binary>>
  }
}

Runtime.print_metadata(metadata)

Benchee.run(
  %{
    "MB-001 Key Construction" => fn %{key_tuple: {key_namespace, key_set, user_key}} ->
      Key.new(key_namespace, key_set, user_key)
    end,
    "MB-002 Key Digest Generation" => fn %{digest_payload: payload} ->
      :crypto.hash(:ripemd160, payload)
    end
  },
  Aerospike.Bench.benchee_options(
    config,
    title: "L1 microbenchmark baseline",
    inputs: inputs,
    print: [benchmarking: false, configuration: false, fast_warning: false]
  )
)
