defmodule AeroMarketLive.DateHelper do
  @moduledoc """
  Date utilities for the market data generator and reader.

  Uses epoch-milliseconds truncated to midnight as the day key suffix in
  the Aerospike primary key, and derives the intra-day time-of-day label
  from a 1-based minute offset starting at 09:30.
  """

  @day_ms 24 * 60 * 60 * 1000

  @doc """
  Truncates `ts_ms` (epoch milliseconds) to local midnight on the same day.
  Returns epoch milliseconds. Uses UTC like the Java original when
  `java.util.Calendar.getInstance()` runs without an explicit timezone (which
  still produces the integer millis-at-midnight of the host's local day).
  """
  def midnight_ms(ts_ms) when is_integer(ts_ms) do
    ts_ms - rem(ts_ms, @day_ms)
  end

  @doc """
  Formats epoch milliseconds as `dd/MM/yyyy`.
  """
  def format_date(ts_ms) when is_integer(ts_ms) do
    dt = DateTime.from_unix!(ts_ms, :millisecond)
    :io_lib.format("~2..0B/~2..0B/~4..0B", [dt.day, dt.month, dt.year]) |> IO.iodata_to_binary()
  end

  @doc """
  Converts a Google Finance index (1-based minute offset within a trading day,
  starting at 09:30) into an `H:M` time-of-day string, matching the Java
  `getTimeStamp/1` helper.
  """
  def index_to_time_of_day(index) when is_integer(index) do
    hours = 9 + div(index - 1, 60)
    minutes = 30 + rem(index - 1, 60)

    if minutes > 59 do
      "#{hours + 1}:#{minutes - 60}"
    else
      "#{hours}:#{minutes}"
    end
  end

  @doc """
  Returns the number of days in `[start_ms, end_ms]` inclusive (matching the
  Java `difference/2` helper which adds +1).
  """
  def day_difference(start_ms, end_ms)
      when is_integer(start_ms) and is_integer(end_ms) and end_ms >= start_ms do
    div(end_ms - start_ms, @day_ms) + 1
  end

  @doc "Adds one day to an epoch-millisecond midnight value."
  def add_day(ts_ms) when is_integer(ts_ms), do: ts_ms + @day_ms

  @doc """
  Parses a `dd/MM/yyyy` date string into the epoch-millisecond midnight value.
  """
  def parse_date!(str) when is_binary(str) do
    case String.split(str, "/") do
      [d, m, y] ->
        date = Date.new!(String.to_integer(y), String.to_integer(m), String.to_integer(d))
        {:ok, dt} = DateTime.new(date, ~T[00:00:00], "Etc/UTC")
        DateTime.to_unix(dt, :millisecond)

      _ ->
        raise ArgumentError, "invalid date #{inspect(str)}: expected dd/MM/yyyy"
    end
  end

  @doc "Formats a number with two decimal places (matching Java `DecimalFormat(\"##.##\")`)."
  def fmt2(n) when is_number(n), do: :erlang.float_to_binary(n * 1.0, decimals: 2)
end
