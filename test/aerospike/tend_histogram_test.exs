defmodule Aerospike.TendHistogramTest do
  use ExUnit.Case, async: true

  import Bitwise, only: [<<<: 2]

  alias Aerospike.TendHistogram

  describe "new/0" do
    test "fresh reference starts at zero count and empty buckets" do
      ref = TendHistogram.new()
      assert TendHistogram.count(ref) == 0
      assert TendHistogram.buckets(ref) == List.duplicate(0, 32)
    end
  end

  describe "record/2" do
    test "sub-microsecond samples land in bucket 0 (≤ 1 µs)" do
      ref = TendHistogram.new()
      TendHistogram.record(ref, 0)
      TendHistogram.record(ref, native_from_microseconds(1))

      assert TendHistogram.count(ref) == 2
      [b0 | rest] = TendHistogram.buckets(ref)
      assert b0 == 2
      assert Enum.all?(rest, &(&1 == 0))
    end

    test "1 ms samples land in the ~1 ms bucket (index 9 for 512..1023 µs, 10 for 1024..)" do
      ref = TendHistogram.new()

      # 1000 µs = 2^9.96… → bit_length(1000) - 1 = 9 (1000 is 10 bits).
      TendHistogram.record(ref, native_from_microseconds(1_000))
      buckets = TendHistogram.buckets(ref)
      assert Enum.at(buckets, 9) == 1
      assert TendHistogram.count(ref) == 1
    end

    test "negative durations are clamped into bucket 0" do
      ref = TendHistogram.new()
      TendHistogram.record(ref, -1_000)

      [b0 | rest] = TendHistogram.buckets(ref)
      assert b0 == 1
      assert Enum.all?(rest, &(&1 == 0))
      assert TendHistogram.count(ref) == 1
    end

    test "extreme samples saturate at bucket 31 without raising" do
      ref = TendHistogram.new()
      huge = native_from_microseconds(1 <<< 40)

      TendHistogram.record(ref, huge)
      buckets = TendHistogram.buckets(ref)

      assert List.last(buckets) == 1
      # Every intermediate bucket stays empty.
      assert Enum.drop(buckets, -1) |> Enum.all?(&(&1 == 0))
      assert TendHistogram.count(ref) == 1
    end

    test "samples distribute across the expected bucket for a known µs value" do
      ref = TendHistogram.new()

      # 100 µs → bit_length(100) - 1 = 6 (100 is 7 bits).
      TendHistogram.record(ref, native_from_microseconds(100))
      buckets = TendHistogram.buckets(ref)
      assert Enum.at(buckets, 6) == 1
    end
  end

  describe "percentile/2" do
    test "returns nil on an empty histogram" do
      ref = TendHistogram.new()
      assert TendHistogram.percentile(ref, 0.5) == nil
      assert TendHistogram.percentile(ref, 0.99) == nil
    end

    test "returns a mid-bucket estimate for a single bucket" do
      ref = TendHistogram.new()

      # Ten samples all in bucket 6 (64..127 µs).
      for _ <- 1..10 do
        TendHistogram.record(ref, native_from_microseconds(100))
      end

      p50 = TendHistogram.percentile(ref, 0.5)
      # Mid-bucket for bucket 6 is 64 + 64/2 = 96 µs.
      assert p50 == 96
    end

    test "walks buckets in order until cumulative count meets the target" do
      ref = TendHistogram.new()

      # 9 samples in bucket 6 (≈ 100 µs) and 1 sample in bucket 15 (≈ 50 ms).
      for _ <- 1..9, do: TendHistogram.record(ref, native_from_microseconds(100))
      TendHistogram.record(ref, native_from_microseconds(50_000))

      # p50 sits inside the 9-sample bucket.
      assert TendHistogram.percentile(ref, 0.5) == 96

      # p99 is 9.9 → ceil = 10th sample, which is in the slow bucket.
      # Bucket 15 mid = 32_768 + 16_384 = 49_152 µs.
      assert TendHistogram.percentile(ref, 0.99) == 49_152
    end
  end

  describe "concurrent writers" do
    test "100 parallel record calls do not lose samples" do
      ref = TendHistogram.new()

      parent = self()

      tasks =
        for i <- 1..100 do
          Task.async(fn ->
            TendHistogram.record(ref, native_from_microseconds(500))
            send(parent, {:done, i})
          end)
        end

      Enum.each(tasks, &Task.await/1)
      assert TendHistogram.count(ref) == 100
    end
  end

  defp native_from_microseconds(us) do
    System.convert_time_unit(us, :microsecond, :native)
  end
end
