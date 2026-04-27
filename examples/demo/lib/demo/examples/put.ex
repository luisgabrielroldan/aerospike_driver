defmodule Demo.Examples.Put do
  @moduledoc """
  Demonstrates writing records with string and integer keys.

  """

  require Logger

  @repo Demo.PrimaryClusterRepo
  @namespace "test"
  @set "demo"

  def run do
    # Put with string key
    skey = "putkey_string"
    key = Aerospike.key(@namespace, @set, skey)
    bins = %{"bin1" => "value1", "bin2" => 42}

    Logger.info("  Put: ns=#{@namespace} set=#{@set} key=#{skey} bins=#{inspect(bins)}")
    :ok = @repo.put!(key, bins)
    Logger.info("  Record written: ns=#{@namespace} set=#{@set} key=#{skey}")

    # Put with integer key
    ikey = 12_345
    int_key = Aerospike.key(@namespace, @set, ikey)
    int_bins = %{"name" => "integer_key_record", "count" => 100}

    Logger.info("  Put: ns=#{@namespace} set=#{@set} key=#{ikey} bins=#{inspect(int_bins)}")
    :ok = @repo.put!(int_key, int_bins)
    Logger.info("  Record written: ns=#{@namespace} set=#{@set} key=#{ikey}")

    # Cleanup
    @repo.delete(key)
    @repo.delete(int_key)
  end
end
