Code.require_file("../../bench_helper.exs", __DIR__)

defmodule Aerospike.Bench.Micro.BatchResultConversion do
  @moduledoc false

  alias Aerospike.BatchResult
  alias Aerospike.Bench.Support.Runtime
  alias Aerospike.Command.Batch
  alias Aerospike.Command.BatchCommand.Result
  alias Aerospike.Command.BatchGet
  alias Aerospike.Error
  alias Aerospike.Key
  alias Aerospike.Record

  @namespace "bench"
  @set "batch_result_conversion"
  @batch_sizes [1_000, 10_000]
  @iterations 32

  def run do
    config = Aerospike.Bench.load_config()
    metadata = Aerospike.Bench.run_metadata(config, %{iterations_per_sample: @iterations})

    Runtime.print_metadata(metadata, %{workload: :batch_result_conversion_micro})

    Benchee.run(
      jobs(),
      Aerospike.Bench.benchee_options(
        config,
        title: "L1 batch result conversion",
        inputs: inputs(),
        print: [benchmarking: false, configuration: false, fast_warning: false]
      )
    )
  end

  defp jobs do
    %{
      "BRC-001 Mixed batch structs x#{@iterations}" => fn %{mixed_results: results} ->
        repeat(results, &Batch.to_public_results/1)
      end,
      "BRC-002 Batch get records x#{@iterations}" => fn %{read_results: results} ->
        repeat(results, &BatchGet.to_public_results(&1, :all))
      end,
      "BRC-003 Batch get header tuples x#{@iterations}" => fn %{header_results: results} ->
        repeat(results, &BatchGet.to_public_results(&1, :header))
      end,
      "BRC-004 Batch exists tuples x#{@iterations}" => fn %{exists_results: results} ->
        repeat(results, &BatchGet.to_public_results(&1, :exists))
      end
    }
  end

  defp inputs do
    Map.new(@batch_sizes, fn batch_size ->
      {"#{batch_size} results", build_input(batch_size)}
    end)
  end

  defp build_input(batch_size) do
    %{
      mixed_results:
        Enum.map(1..@iterations, fn iteration -> mixed_results(batch_size, iteration) end),
      read_results:
        Enum.map(1..@iterations, fn iteration -> read_results(batch_size, iteration) end),
      header_results:
        Enum.map(1..@iterations, fn iteration -> header_results(batch_size, iteration) end),
      exists_results:
        Enum.map(1..@iterations, fn iteration -> exists_results(batch_size, iteration) end)
    }
  end

  defp mixed_results(batch_size, iteration) do
    Enum.map(1..batch_size, fn index ->
      case rem(index, 4) do
        0 -> ok_result(index, iteration, :delete, nil)
        1 -> ok_result(index, iteration, :read, record(index, iteration))
        2 -> error_result(index, iteration, :udf, udf_error())
        3 -> error_result(index, iteration, :read, :no_master)
      end
    end)
  end

  defp read_results(batch_size, iteration) do
    Enum.map(1..batch_size, fn index ->
      if rem(index, 8) == 0 do
        error_result(index, iteration, :read, key_not_found())
      else
        ok_result(index, iteration, :read, record(index, iteration))
      end
    end)
  end

  defp header_results(batch_size, iteration) do
    Enum.map(1..batch_size, fn index ->
      if rem(index, 8) == 0 do
        error_result(index, iteration, :read_header, key_not_found())
      else
        ok_result(index, iteration, :read_header, %{generation: rem(index, 65_535), ttl: 3_600})
      end
    end)
  end

  defp exists_results(batch_size, iteration) do
    Enum.map(1..batch_size, fn index ->
      if rem(index, 8) == 0 do
        error_result(index, iteration, :exists, key_not_found())
      else
        ok_result(index, iteration, :exists, nil)
      end
    end)
  end

  defp repeat(inputs, fun) do
    Enum.reduce(inputs, nil, fn input, _acc -> fun.(input) end)
  end

  defp ok_result(index, iteration, kind, record) do
    %Result{
      index: index - 1,
      key: key(index, iteration),
      kind: kind,
      status: :ok,
      record: record,
      error: nil,
      in_doubt: false
    }
  end

  defp error_result(index, iteration, kind, error) do
    %Result{
      index: index - 1,
      key: key(index, iteration),
      kind: kind,
      status: :error,
      record: nil,
      error: error,
      in_doubt: false
    }
  end

  defp record(index, iteration) do
    %Record{
      key: key(index, iteration),
      bins: %{"name" => "Ada #{iteration}:#{index}", "count" => index},
      generation: rem(index, 65_535),
      ttl: 3_600
    }
  end

  defp key(index, iteration), do: Key.new(@namespace, @set, "brc:#{iteration}:#{index}")

  defp key_not_found, do: Error.from_result_code(:key_not_found)
  defp udf_error, do: Error.from_result_code(:udf_bad_response)

  def ensure_shapes! do
    input = build_input(8)

    [%BatchResult{} | _] = Batch.to_public_results(hd(input.mixed_results))
    [{:ok, %Record{}} | _] = BatchGet.to_public_results(hd(input.read_results), :all)

    [{:ok, %Record{bins: %{}}} | _] =
      BatchGet.to_public_results(hd(input.header_results), :header)

    [{:ok, true} | _] = BatchGet.to_public_results(hd(input.exists_results), :exists)

    :ok
  end
end

Aerospike.Bench.Micro.BatchResultConversion.ensure_shapes!()
Aerospike.Bench.Micro.BatchResultConversion.run()
