Code.require_file("../../bench_helper.exs", __DIR__)

defmodule Aerospike.Bench.Micro.ScanResponse do
  @moduledoc false

  alias Aerospike.Bench.Support.Runtime
  alias Aerospike.Protocol.AsmMsg
  alias Aerospike.Protocol.AsmMsg.Field
  alias Aerospike.Protocol.AsmMsg.Operation
  alias Aerospike.Protocol.ScanResponse

  @namespace "bench"
  @set "scan_response"
  @record_counts [1_000, 10_000]

  def run do
    config = Aerospike.Bench.load_config()
    metadata = Aerospike.Bench.run_metadata(config)

    Runtime.print_metadata(metadata, %{workload: :scan_response_micro})

    Benchee.run(
      jobs(),
      Aerospike.Bench.benchee_options(
        config,
        title: "L1 scan response parsing",
        inputs: inputs(),
        print: [benchmarking: false, configuration: false, fast_warning: false]
      )
    )
  end

  defp jobs do
    %{
      "SR-001 parse_stream_chunk" => fn %{body: body} ->
        {:ok, _records, _parts, true} = ScanResponse.parse_stream_chunk(body, @namespace, @set)
      end,
      "SR-002 parse" => fn %{body: body} ->
        {:ok, _records, _parts} = ScanResponse.parse(body, @namespace, @set)
      end,
      "SR-003 count_records" => fn %{body: body} ->
        {:ok, _count} = ScanResponse.count_records(body)
      end
    }
  end

  defp inputs do
    Map.new(@record_counts, fn record_count ->
      {"#{record_count} records", %{body: scan_body(record_count)}}
    end)
  end

  defp scan_body(record_count) do
    records =
      1..record_count
      |> Enum.map(&record_msg/1)
      |> Enum.map(&AsmMsg.encode/1)

    IO.iodata_to_binary([records, AsmMsg.encode(last_msg())])
  end

  defp record_msg(index) do
    %AsmMsg{
      info1: AsmMsg.info1_read(),
      result_code: 0,
      generation: rem(index, 65_535),
      expiration: 0,
      fields: [
        Field.namespace(@namespace),
        Field.set(@set),
        Field.digest(digest(index))
      ],
      operations: [
        %Operation{
          op_type: Operation.op_read(),
          particle_type: 1,
          bin_name: "i",
          data: <<index::64-signed-big>>
        },
        %Operation{
          op_type: Operation.op_read(),
          particle_type: 3,
          bin_name: "name",
          data: "record-#{index}"
        }
      ]
    }
  end

  defp last_msg do
    %AsmMsg{
      info3: AsmMsg.info3_last(),
      result_code: 0,
      fields: [],
      operations: []
    }
  end

  defp digest(index), do: :crypto.hash(:ripemd160, <<index::64-big>>)
end

Aerospike.Bench.Micro.ScanResponse.run()
