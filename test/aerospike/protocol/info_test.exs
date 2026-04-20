defmodule Aerospike.Protocol.InfoTest do
  use ExUnit.Case, async: true

  alias Aerospike.Protocol.Info

  doctest Info

  describe "parse_features/1" do
    test "recognised tokens become atoms" do
      assert MapSet.new([:compression, :pipelining]) ==
               Info.parse_features("compression;pipelining")
    end

    test "single recognised token" do
      assert MapSet.new([:compression]) == Info.parse_features("compression")
    end

    test "unknown tokens are preserved as {:unknown, raw} tuples" do
      result = Info.parse_features("compression;batch-index;peers")

      assert MapSet.member?(result, :compression)
      assert MapSet.member?(result, {:unknown, "batch-index"})
      assert MapSet.member?(result, {:unknown, "peers"})
      assert MapSet.size(result) == 3
    end

    test "empty body produces an empty MapSet" do
      assert MapSet.new() == Info.parse_features("")
    end

    test "stray separators do not produce empty tokens" do
      assert MapSet.new([:compression]) == Info.parse_features(";compression;")
      assert MapSet.new([:compression]) == Info.parse_features("compression;;")
      assert MapSet.new() == Info.parse_features(";;;")
    end

    test "whitespace around tokens is trimmed" do
      assert MapSet.new([:compression, :pipelining]) ==
               Info.parse_features(" compression ; pipelining ")
    end

    test "duplicate tokens collapse to one entry" do
      assert MapSet.new([:compression]) == Info.parse_features("compression;compression")
    end
  end
end
