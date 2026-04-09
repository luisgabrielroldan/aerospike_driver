Code.require_file("../../bench_helper.exs", __DIR__)

alias Aerospike.Bench.Support.Runtime
alias Aerospike.Protocol.BatchResponse

defmodule Aerospike.Bench.Support.BatchDecodeFixture do
  @moduledoc false

  alias Aerospike.Batch
  alias Aerospike.Key
  alias Aerospike.Protocol.AsmMsg
  alias Aerospike.Protocol.AsmMsg.Operation

  def batch_exists_body(count) do
    Enum.map_join(0..(count - 1), &exists_entry/1) <> last_header()
  end

  def batch_operate_body(count) do
    Enum.map_join(0..(count - 1), &operate_entry/1) <> last_header()
  end

  def batch_read_ops(count) do
    Enum.map(0..(count - 1), fn index ->
      Batch.read(Key.new("bench", "micro", "mb009:#{index}"))
    end)
  end

  defp exists_entry(index), do: header(rc: exists_rc(index), bidx: index)

  defp operate_entry(index) do
    rc = operate_rc(index)

    if rc == 0 do
      header(rc: 0, bidx: index, oc: 1) <> success_operation(index)
    else
      header(rc: rc, bidx: index, oc: 0)
    end
  end

  defp success_operation(index) do
    Operation.encode(%Operation{
      op_type: Operation.op_read(),
      particle_type: Operation.particle_integer(),
      bin_name: "counter",
      data: <<index::64-big-signed>>
    })
  end

  defp exists_rc(index), do: if(rem(index, 5) == 0, do: 2, else: 0)
  defp operate_rc(index), do: if(rem(index, 5) == 0, do: 2, else: 0)

  defp header(opts) do
    i3 = Keyword.get(opts, :i3, 0)
    rc = Keyword.get(opts, :rc, 0)
    bidx = Keyword.get(opts, :bidx, 0)
    fc = Keyword.get(opts, :fc, 0)
    oc = Keyword.get(opts, :oc, 0)

    <<
      22::8,
      0::8,
      0::8,
      i3::8,
      0::8,
      rc::8,
      0::32-big,
      0::32-big,
      bidx::32-big,
      fc::16-big,
      oc::16-big
    >>
  end

  defp last_header do
    header(i3: AsmMsg.info3_last())
  end
end

config = Aerospike.Bench.load_config()
metadata = Aerospike.Bench.run_metadata(config)

count_100 = 100
count_1000 = 1000

exists_body_100 = Aerospike.Bench.Support.BatchDecodeFixture.batch_exists_body(count_100)
exists_body_1000 = Aerospike.Bench.Support.BatchDecodeFixture.batch_exists_body(count_1000)

ops_100 = Aerospike.Bench.Support.BatchDecodeFixture.batch_read_ops(count_100)
ops_1000 = Aerospike.Bench.Support.BatchDecodeFixture.batch_read_ops(count_1000)

operate_body_100 = Aerospike.Bench.Support.BatchDecodeFixture.batch_operate_body(count_100)
operate_body_1000 = Aerospike.Bench.Support.BatchDecodeFixture.batch_operate_body(count_1000)

Runtime.print_metadata(metadata, %{workload: :mb_009})

Benchee.run(
  %{
    "batch_exists 100" => fn ->
      {:ok, _exists} = BatchResponse.parse_batch_exists(exists_body_100, count_100)
    end,
    "batch_exists 1000" => fn ->
      {:ok, _exists} = BatchResponse.parse_batch_exists(exists_body_1000, count_1000)
    end,
    "batch_operate_mixed 100" => fn ->
      {:ok, _results} = BatchResponse.parse_batch_operate(operate_body_100, ops_100)
    end,
    "batch_operate_mixed 1000" => fn ->
      {:ok, _results} = BatchResponse.parse_batch_operate(operate_body_1000, ops_1000)
    end
  },
  Aerospike.Bench.benchee_options(
    config,
    title: "L1 batch decode microbenchmark",
    print: [benchmarking: false, configuration: false, fast_warning: false]
  )
)
