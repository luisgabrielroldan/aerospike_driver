defmodule Demo.Examples.Put do
  @moduledoc """
  Demonstrates writing records with string and integer keys.

  """

  require Logger

  @conn :aero
  @namespace "test"
  @set "demo"

  def run do
    # Put with string key
    skey = "putkey_string"
    key = Aerospike.key(@namespace, @set, skey)
    bins = %{"bin1" => "value1", "bin2" => 42}

    Logger.info("  Put: ns=#{@namespace} set=#{@set} key=#{skey} bins=#{inspect(bins)}")
    :ok = Aerospike.put!(@conn, key, bins)
    Logger.info("  Record written: ns=#{@namespace} set=#{@set} key=#{skey}")

    # Put with integer key
    ikey = 12_345
    int_key = Aerospike.key(@namespace, @set, ikey)
    int_bins = %{"name" => "integer_key_record", "count" => 100}

    Logger.info("  Put: ns=#{@namespace} set=#{@set} key=#{ikey} bins=#{inspect(int_bins)}")
    :ok = Aerospike.put!(@conn, int_key, int_bins)
    Logger.info("  Record written: ns=#{@namespace} set=#{@set} key=#{ikey}")

    # Cleanup
    Aerospike.delete(@conn, key)
    Aerospike.delete(@conn, int_key)
  end
end
