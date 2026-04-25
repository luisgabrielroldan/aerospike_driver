defmodule Aerospike.Cluster.TendHistogram do
  @moduledoc false

  @num_buckets 32
  @count_slot @num_buckets + 1
  @num_slots @num_buckets + 1

  @type t :: :atomics.atomics_ref()

  @doc """
  Allocates a fresh histogram reference with every slot set to zero.
  """
  @spec new() :: t()
  def new do
    :atomics.new(@num_slots, signed: false)
  end

  @doc """
  Records one sample. `duration_native` is the difference of two
  `System.monotonic_time/0` readings (native time units); the value
  is converted to microseconds internally.

  Negative or zero durations land in bucket 1 (the `≤ 1 µs` bucket).
  Durations that exceed the top bucket saturate at bucket 32.
  """
  @spec record(t(), integer()) :: :ok
  def record(ref, duration_native) when is_integer(duration_native) do
    bucket = duration_native |> to_microseconds() |> bucket_for()
    :atomics.add(ref, bucket, 1)
    :atomics.add(ref, @count_slot, 1)
    :ok
  end

  @doc """
  Returns the total number of samples recorded since the reference
  was allocated.
  """
  @spec count(t()) :: non_neg_integer()
  def count(ref) do
    :atomics.get(ref, @count_slot)
  end

  @doc """
  Returns the counts per bucket as a list of length
  `#{@num_buckets}`. Bucket index `i` (0-based) corresponds to
  samples in the `[2^i µs, 2^(i+1) µs)` range.
  """
  @spec buckets(t()) :: [non_neg_integer()]
  def buckets(ref) do
    for slot <- 1..@num_buckets, do: :atomics.get(ref, slot)
  end

  @doc """
  Estimates the sample at percentile `p` (between `0.0` and `1.0`) in
  microseconds. Returns `nil` for an empty histogram.

  The estimate is mid-bucket: for a sample landing in bucket `i`
  (covering `[2^i, 2^(i+1))` µs), the returned value is `floor(1.5 * 2^i)`,
  which is the midpoint of the bucket's range. This is a coarse
  approximation; the bucket edges double in each step, so the error
  bound also doubles per step.
  """
  @spec percentile(t(), float()) :: non_neg_integer() | nil
  def percentile(ref, p) when is_float(p) and p >= 0.0 and p <= 1.0 do
    total = count(ref)

    if total == 0 do
      nil
    else
      target = max(1, ceil_int(p * total))
      find_percentile_bucket(ref, target)
    end
  end

  defp find_percentile_bucket(ref, target) do
    Enum.reduce_while(1..@num_buckets, 0, fn slot, acc ->
      running = acc + :atomics.get(ref, slot)

      if running >= target do
        {:halt, bucket_midpoint(slot - 1)}
      else
        {:cont, running}
      end
    end)
  end

  defp bucket_midpoint(index) do
    low = bit_shift_left(1, index)
    # Mid-bucket = low + (high - low) / 2 = low * 1.5. `high` doubles
    # `low` by definition (`2^(i+1) - 2^i = 2^i`), so `floor(low * 1.5)`
    # lands in the bucket's range without forcing float arithmetic.
    low + div(low, 2)
  end

  defp bucket_for(duration_us) when duration_us <= 1, do: 1

  defp bucket_for(duration_us) do
    # `floor(log2(n))` via bit width: the number of bits needed to
    # represent `n` minus one. Clamp into the 0..31 range so
    # pathologically long cycles saturate rather than panic the
    # :atomics bounds check.
    index =
      duration_us
      |> bit_length()
      |> Kernel.-(1)
      |> min(@num_buckets - 1)
      |> max(0)

    index + 1
  end

  defp to_microseconds(native) do
    max(0, System.convert_time_unit(native, :native, :microsecond))
  end

  defp bit_length(n) when is_integer(n) and n > 0 do
    # Elixir's `Integer.digits(n, 2) |> length/1` is correct but
    # allocates a list; the loop here stays on the stack.
    bit_length_loop(n, 0)
  end

  defp bit_length_loop(0, acc), do: acc
  defp bit_length_loop(n, acc), do: bit_length_loop(bsr(n, 1), acc + 1)

  defp bsr(n, k), do: :erlang.bsr(n, k)

  defp bit_shift_left(n, k), do: :erlang.bsl(n, k)

  defp ceil_int(x) when is_float(x), do: trunc(Float.ceil(x))
end
