defmodule Demo.Examples.Udf do
  @moduledoc """
  Demonstrates UDF (User Defined Function) registration, execution, and removal.

  Registers an inline Lua module, executes it on a record via `apply_udf/5`,
  and then removes the package from the server.
  """

  require Logger

  @conn :aero
  @namespace "test"
  @set "demo_udf"
  @package "demo_udf_mod"

  @lua_source """
  function double_bin(rec, bin_name)
    return rec[bin_name] * 2
  end

  function add_bins(rec, bin_a, bin_b)
    return rec[bin_a] + rec[bin_b]
  end

  function put_value(rec, bin_name, value)
    rec[bin_name] = value
    aerospike:update(rec)
  end
  """

  def run do
    register_udf()
    write_record()
    execute_double()
    execute_add_bins()
    execute_write_udf()
    remove_udf()
    cleanup()
  end

  defp register_udf do
    Logger.info("  Registering UDF package '#{@package}.lua'...")

    {:ok, task} = Aerospike.register_udf(@conn, @lua_source, "#{@package}.lua")
    :ok = Aerospike.RegisterTask.wait(task, timeout: 10_000)

    Logger.info("  UDF package registered.")
  end

  defp write_record do
    Logger.info("  Writing record: x=10, y=25")
    key = key("udf_test")
    :ok = Aerospike.put!(@conn, key, %{"x" => 10, "y" => 25})
  end

  defp execute_double do
    Logger.info("  apply_udf: double_bin(x) — expect 20...")

    {:ok, result} = Aerospike.apply_udf(@conn, key("udf_test"), @package, "double_bin", ["x"])

    unless result == 20 do
      raise "Expected double_bin(x)=20, got #{inspect(result)}"
    end

    Logger.info("    Result: #{result}")
  end

  defp execute_add_bins do
    Logger.info("  apply_udf: add_bins(x, y) — expect 35...")

    {:ok, result} =
      Aerospike.apply_udf(@conn, key("udf_test"), @package, "add_bins", ["x", "y"])

    unless result == 35 do
      raise "Expected add_bins(x, y)=35, got #{inspect(result)}"
    end

    Logger.info("    Result: #{result}")
  end

  defp execute_write_udf do
    Logger.info("  apply_udf: put_value(z, 99) — write a bin via UDF...")

    {:ok, _} =
      Aerospike.apply_udf(@conn, key("udf_test"), @package, "put_value", ["z", 99])

    {:ok, record} = Aerospike.get(@conn, key("udf_test"))

    unless record.bins["z"] == 99 do
      raise "Expected z=99 after put_value UDF, got #{inspect(record.bins["z"])}"
    end

    Logger.info("    Verified: z=#{record.bins["z"]} written by UDF")
  end

  defp remove_udf do
    Logger.info("  Removing UDF package '#{@package}.lua'...")
    :ok = Aerospike.remove_udf(@conn, "#{@package}.lua")
    Logger.info("  UDF package removed.")
  end

  defp cleanup do
    Aerospike.delete(@conn, key("udf_test"))
  end

  defp key(id), do: Aerospike.key(@namespace, @set, id)
end
