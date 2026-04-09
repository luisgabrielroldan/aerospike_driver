Code.require_file("../../bench_helper.exs", __DIR__)

alias Aerospike.Bench.Support.Runtime
alias Aerospike.Key
alias Aerospike.Protocol.AsmMsg
alias Aerospike.Protocol.AsmMsg.Value
alias Aerospike.Protocol.Message

config = Aerospike.Bench.load_config()
metadata = Aerospike.Bench.run_metadata(config)

namespace = "bench"
set = "micro"

inputs = %{
  "small/1bin/128B" => %{
    read: %{
      namespace: namespace,
      set: set,
      digest: Key.new(namespace, set, "mb005:small").digest
    },
    write: %{
      namespace: namespace,
      set: set,
      digest: Key.new(namespace, set, "mb006:small").digest,
      operations: Value.encode_bin_operations(%{"payload" => String.duplicate("s", 128)})
    }
  },
  "medium/4bins/512B" => %{
    read: %{
      namespace: namespace,
      set: set,
      digest: Key.new(namespace, set, "mb005:medium").digest
    },
    write: %{
      namespace: namespace,
      set: set,
      digest: Key.new(namespace, set, "mb006:medium").digest,
      operations:
        Value.encode_bin_operations(%{
          "payload_1" => String.duplicate("m", 128),
          "payload_2" => String.duplicate("n", 128),
          "payload_3" => String.duplicate("o", 128),
          "payload_4" => String.duplicate("p", 128)
        })
    }
  }
}

Runtime.print_metadata(metadata, %{workload: :mb_005_mb_006})

Benchee.run(
  %{
    "MB-005 Command Assembly (Read)" => fn %{read: read_input} ->
      read_input
      |> then(fn %{namespace: ns, set: set_name, digest: digest} ->
        AsmMsg.read_command(ns, set_name, digest)
      end)
      |> AsmMsg.encode()
      |> Message.encode_as_msg_iodata()
    end,
    "MB-005a Command Assembly (Exists)" => fn %{read: read_input} ->
      read_input
      |> then(fn %{namespace: ns, set: set_name, digest: digest} ->
        AsmMsg.exists_command(ns, set_name, digest)
      end)
      |> AsmMsg.encode()
      |> Message.encode_as_msg_iodata()
    end,
    "MB-006a Command Assembly (Touch)" => fn %{read: read_input} ->
      read_input
      |> then(fn %{namespace: ns, set: set_name, digest: digest} ->
        AsmMsg.touch_command(ns, set_name, digest)
      end)
      |> AsmMsg.encode()
      |> Message.encode_as_msg_iodata()
    end,
    "MB-006b Command Assembly (Delete)" => fn %{read: read_input} ->
      read_input
      |> then(fn %{namespace: ns, set: set_name, digest: digest} ->
        AsmMsg.delete_command(ns, set_name, digest)
      end)
      |> AsmMsg.encode()
      |> Message.encode_as_msg_iodata()
    end,
    "MB-006 Command Assembly (Write)" => fn %{write: write_input} ->
      write_input
      |> then(fn %{namespace: ns, set: set_name, digest: digest, operations: operations} ->
        AsmMsg.write_command(ns, set_name, digest, operations)
      end)
      |> AsmMsg.encode()
      |> Message.encode_as_msg_iodata()
    end
  },
  Aerospike.Bench.benchee_options(
    config,
    title: "L1 command assembly microbenchmark",
    inputs: inputs,
    print: [benchmarking: false, configuration: false, fast_warning: false]
  )
)
