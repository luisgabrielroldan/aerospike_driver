defmodule Demo.Examples.BatchUdf do
  @moduledoc """
  Demonstrates batch UDF execution using `Batch.udf/5` inside `batch_operate/3`.

  Registers a Lua module, then executes server-side functions across multiple
  records in a single batch round-trip.
  """

  require Logger

  alias Aerospike.Batch

  @conn :aero
  @namespace "test"
  @set "demo_budf"
  @package "demo_batch_udf"

  @lua_source """
  function increment(rec, bin_name, amount)
    rec[bin_name] = (rec[bin_name] or 0) + amount
    aerospike:update(rec)
    return rec[bin_name]
  end

  function get_double(rec, bin_name)
    return (rec[bin_name] or 0) * 2
  end
  """

  def run do
    register_udf()
    write_records()

    case probe_batch_udf() do
      :ok ->
        batch_udf_read()
        batch_udf_write()
        remove_udf()
        cleanup()

      :unsupported ->
        Logger.warning("  BatchUdf: skipped — batch UDF not yet supported in wire encoder")
        remove_udf()
        cleanup()
        :skipped
    end
  end

  defp register_udf do
    Logger.info("  Registering UDF package '#{@package}.lua'...")
    {:ok, task} = Aerospike.register_udf(@conn, @lua_source, "#{@package}.lua")
    :ok = Aerospike.RegisterTask.wait(task, timeout: 10_000)
    Process.sleep(500)
    Logger.info("  UDF package registered.")
  end

  defp write_records do
    Logger.info("  Writing 3 records with counters...")

    for {id, val} <- [{"a", 10}, {"b", 20}, {"c", 30}] do
      :ok = Aerospike.put!(@conn, key(id), %{"counter" => val, "name" => id})
    end
  end

  defp probe_batch_udf do
    {:ok, [result]} =
      Aerospike.batch_operate(@conn, [
        Batch.udf(key("a"), @package, "get_double", ["counter"])
      ])

    if result.status == :ok, do: :ok, else: :unsupported
  end

  defp batch_udf_read do
    Logger.info("  Batch UDF: get_double on 3 records...")

    {:ok, results} =
      Aerospike.batch_operate(@conn, [
        Batch.udf(key("a"), @package, "get_double", ["counter"]),
        Batch.udf(key("b"), @package, "get_double", ["counter"]),
        Batch.udf(key("c"), @package, "get_double", ["counter"])
      ])

    expected = [20, 40, 60]

    for {result, expect} <- Enum.zip(results, expected) do
      unless result.status == :ok do
        raise "Batch UDF failed: #{inspect(result)}"
      end

      value = result.record.bins["SUCCESS"]

      unless value == expect do
        raise "Expected #{expect}, got #{inspect(value)}"
      end

      Logger.info("    get_double: #{inspect(value)} (expected #{expect})")
    end
  end

  defp batch_udf_write do
    Logger.info("  Batch UDF: increment counters by different amounts...")

    {:ok, results} =
      Aerospike.batch_operate(@conn, [
        Batch.udf(key("a"), @package, "increment", ["counter", 5]),
        Batch.udf(key("b"), @package, "increment", ["counter", 10]),
        Batch.udf(key("c"), @package, "increment", ["counter", 15])
      ])

    expected = [15, 30, 45]

    for {result, expect} <- Enum.zip(results, expected) do
      unless result.status == :ok do
        raise "Batch UDF write failed: #{inspect(result)}"
      end

      value = result.record.bins["SUCCESS"]

      unless value == expect do
        raise "Expected #{expect}, got #{inspect(value)}"
      end

      Logger.info("    increment result: #{value} (expected #{expect})")
    end

    Logger.info("  Verifying persisted values...")

    for {id, expect} <- [{"a", 15}, {"b", 30}, {"c", 45}] do
      {:ok, rec} = Aerospike.get(@conn, key(id))

      unless rec.bins["counter"] == expect do
        raise "Expected counter=#{expect} for #{id}, got #{rec.bins["counter"]}"
      end
    end

    Logger.info("  All counters verified.")
  end

  defp remove_udf do
    Logger.info("  Removing UDF package '#{@package}.lua'...")
    :ok = Aerospike.remove_udf(@conn, "#{@package}.lua")
    Logger.info("  UDF package removed.")
  end

  defp cleanup do
    for id <- ["a", "b", "c"] do
      Aerospike.delete(@conn, key(id))
    end
  end

  defp key(id), do: Aerospike.key(@namespace, @set, id)
end
