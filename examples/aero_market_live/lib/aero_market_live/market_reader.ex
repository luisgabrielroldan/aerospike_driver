defmodule AeroMarketLive.MarketReader do
  @moduledoc """
  Structured read pipeline for the LiveView dashboard.

  Structured read pipeline for the LiveView dashboard.

  Every fetch uses server-side rank operations to derive day statistics
  from the CDT map written by `Generator`.

  ## 9-op per-day read

  `fetch_day/3` issues a **single** `operate/4` call that mixes nine
  server-side operations on one `(ticker, day)` record:

    1. `get_by_rank(stock, -1, VALUE)` — day's max value
    2. `get_by_rank(stock, -1, INDEX)` — sorted-map index of the max
    3. `get_by_rank(stock,  0, VALUE)` — day's min value
    4. `get_by_rank(stock,  0, INDEX)` — sorted-map index of the min
    5. `get_by_index(stock,  0, VALUE)` — first tick of the day
    6. `get_by_index(stock, -1, VALUE)` — last tick of the day
    7. `get("sum")` — running sum for the day
    8. `map_size(stock)` — number of ticks
    9. `Op.Exp.read("is_positive", sum > 0.0)` — computed positive-day flag

  Seven ops target `stock`, so we pass `respond_per_each_op: true` and the
  driver returns a 7-element list in the order issued. `sum` stays a
  scalar because only one op targets that bin. `is_positive` is returned as
  its own named bin in the response record.

  ## Multi-day ranges

  `fetch_day_range/3` reads each day with `fetch_day/3`. The current driver
  keeps the heterogeneous batch API narrow and does not expose per-entry
  `respond_per_each_op` options, which this summary shape requires because
  several CDT operations target the same `stock` bin.

  ## Leaderboard

  `fetch_leaderboard/3` reads the `difference` map from the pre-written
  `overallsummary` record, using the same rank-op shape as the CLI's
  `rank_ops/1`: ten ops for `num_tickers >= 5` (ranks -1..-5, each
  returning KEY + VALUE), or four ops for best/worst otherwise.

  ## Writing the overall difference

  `write_overall_diff/4` is the companion write that feeds the
  leaderboard. Call it after `fetch_day/3` for each ticker with the day's
  `DaySummary`, then `fetch_leaderboard/3` to read the ranked view.
  """

  require Logger

  alias Aerospike.Exp
  alias Aerospike.Op
  alias Aerospike.Op.Exp, as: ExpOp
  alias Aerospike.Op.Map, as: MapOp
  alias AeroMarketLive.DateHelper

  @namespace "test"
  @timeseries_set "timeseries"
  @overall_summary_set "overallsummary"

  # KEY_VALUE_ORDERED map with no write-flag constraint — unlike the
  # per-day `stock` map (CREATE_ONLY), the leaderboard's `difference`
  # entries are overwritten every refresh, so we can't use CREATE_ONLY here.
  @leaderboard_policy %{attr: 3, flags: 0}

  defmodule DaySummary do
    @moduledoc """
    Structured result of a single `(ticker, day)` read.

    `max_time` and `min_time` are wall-clock strings derived from the
    sorted-map index via `DateHelper.index_to_time_of_day/1`.
    """

    @enforce_keys [
      :ticker,
      :day_ms,
      :count,
      :sum,
      :avg,
      :start_val,
      :end_val,
      :max,
      :max_time,
      :min,
      :min_time
    ]

    defstruct [
      :ticker,
      :day_ms,
      :count,
      :sum,
      :avg,
      :start_val,
      :end_val,
      :max,
      :max_time,
      :min,
      :min_time,
      is_positive: false
    ]

    @type t :: %__MODULE__{
            ticker: String.t(),
            day_ms: integer(),
            count: non_neg_integer(),
            sum: float(),
            avg: float(),
            start_val: float(),
            end_val: float(),
            max: float(),
            max_time: String.t(),
            min: float(),
            min_time: String.t(),
            is_positive: boolean()
          }
  end

  # ---------------------------------------------------------------------------
  # Public API
  # ---------------------------------------------------------------------------

  @doc """
  Fetches a single `(ticker, day)` summary via the 8-op rank-op pattern.

  Returns `:empty` when the record does not exist or has no ticks yet —
  the LiveView must render an empty state cleanly, so this is not an
  error.
  """
  @spec fetch_day(atom(), String.t(), integer()) :: {:ok, DaySummary.t()} | :empty
  def fetch_day(conn, ticker, day_ms) when is_binary(ticker) and is_integer(day_ms) do
    key = Aerospike.key(@namespace, @timeseries_set, "#{ticker}#{day_ms}")

    case Aerospike.operate(conn, key, day_ops(), respond_per_each_op: true) do
      {:ok, record} -> build_summary(record, ticker, day_ms)
      {:error, _reason} -> :empty
    end
  end

  @doc """
  Fetches per-day summaries for a ticker over the given days.

  Days with no data are dropped — the caller gets only the days that
  actually have ticks.
  """
  @spec fetch_day_range(atom(), String.t(), [integer()]) :: [DaySummary.t()]
  def fetch_day_range(conn, ticker, day_list)
      when is_binary(ticker) and is_list(day_list) do
    day_list
    |> Enum.map(&fetch_day(conn, ticker, &1))
    |> Enum.flat_map(fn
      {:ok, summary} -> [summary]
      :empty -> []
    end)
  end

  @doc """
  Writes a ticker's net position for the day into the `overallsummary`
  record pointed at by `summary_key`.

  The LiveView calls this after each `fetch_day/3` cycle, then reads back
  a ranked view via `fetch_leaderboard/3`. Errors are swallowed — a
  transient write failure must not interrupt the dashboard.
  """
  @spec write_overall_diff(atom(), Aerospike.Key.t(), String.t(), DaySummary.t()) :: :ok
  def write_overall_diff(conn, summary_key, ticker, %DaySummary{} = summary)
      when is_binary(ticker) do
    difference = summary.end_val - summary.start_val

    ops = [MapOp.put("difference", ticker, difference, policy: @leaderboard_policy)]

    case Aerospike.operate(conn, summary_key, ops) do
      {:ok, _record} ->
        :ok

      {:error, reason} ->
        Logger.warning(
          "[MarketReader] write_overall_diff failed for #{ticker}: #{inspect(reason)}"
        )

        :ok
    end
  end

  @doc """
  Reads the top-performing tickers from the `overallsummary` record,
  sorted by net position descending.

  When `num_tickers >= 5`, returns up to five `{ticker, difference}` tuples
  from ranks -1..-5. Otherwise returns best and worst as two tuples.
  Returns `[]` if the summary record is missing.
  """
  @spec fetch_leaderboard(atom(), Aerospike.Key.t(), pos_integer()) :: [
          {String.t(), float()}
        ]
  def fetch_leaderboard(conn, summary_key, num_tickers)
      when is_integer(num_tickers) and num_tickers > 0 do
    ops = rank_ops(num_tickers)

    case Aerospike.operate(conn, summary_key, ops, respond_per_each_op: true) do
      {:ok, record} ->
        record.bins["difference"]
        |> List.wrap()
        |> rank_pairs()

      {:error, _reason} ->
        []
    end
  end

  @doc """
  Builds the `overallsummary` key the LiveView should pass to
  `write_overall_diff/4` and `fetch_leaderboard/3`.

  The CLI uses `System.system_time(:millisecond)` at call time as the PK;
  the LiveView passes a stable value (e.g., today's midnight) so repeated
  refreshes target the same record.
  """
  @spec summary_key(integer() | String.t()) :: Aerospike.Key.t()
  def summary_key(pk), do: Aerospike.key(@namespace, @overall_summary_set, pk)

  # ---------------------------------------------------------------------------
  # Internals
  # ---------------------------------------------------------------------------

  defp day_ops do
    [
      MapOp.get_by_rank("stock", -1, return_type: MapOp.return_value()),
      MapOp.get_by_rank("stock", -1, return_type: MapOp.return_index()),
      MapOp.get_by_rank("stock", 0, return_type: MapOp.return_value()),
      MapOp.get_by_rank("stock", 0, return_type: MapOp.return_index()),
      MapOp.get_by_index("stock", 0, return_type: MapOp.return_value()),
      MapOp.get_by_index("stock", -1, return_type: MapOp.return_value()),
      Op.get("sum"),
      MapOp.size("stock"),
      ExpOp.read("is_positive", Exp.gt(Exp.float_bin("sum"), Exp.val(0.0)))
    ]
  end

  defp build_summary(%{bins: %{"stock" => stock}} = record, ticker, day_ms)
       when is_list(stock) do
    [max_val, max_idx, min_val, min_idx, first_val, last_val, count] = stock

    case count do
      0 ->
        :empty

      n when is_integer(n) and n > 0 ->
        sum = to_float(record.bins["sum"])

        {:ok,
         %DaySummary{
           ticker: ticker,
           day_ms: day_ms,
           count: n,
           sum: sum,
           avg: sum / n,
           start_val: to_float(first_val),
           end_val: to_float(last_val),
           max: to_float(max_val),
           max_time: DateHelper.index_to_time_of_day(max_idx),
           min: to_float(min_val),
           min_time: DateHelper.index_to_time_of_day(min_idx),
           is_positive: record.bins["is_positive"] == true
         }}
    end
  end

  defp build_summary(_record, _ticker, _day_ms), do: :empty

  # With `respond_per_each_op: true` the driver returns an interleaved
  # `[key, value, key, value, ...]` list.
  defp rank_ops(num_tickers) when num_tickers >= 5 do
    Enum.flat_map(1..5, fn r ->
      [
        MapOp.get_by_rank("difference", -r, return_type: MapOp.return_key()),
        MapOp.get_by_rank("difference", -r, return_type: MapOp.return_value())
      ]
    end)
  end

  defp rank_ops(_num_tickers) do
    [
      MapOp.get_by_rank("difference", -1, return_type: MapOp.return_key()),
      MapOp.get_by_rank("difference", -1, return_type: MapOp.return_value()),
      MapOp.get_by_rank("difference", 0, return_type: MapOp.return_key()),
      MapOp.get_by_rank("difference", 0, return_type: MapOp.return_value())
    ]
  end

  defp rank_pairs([]), do: []

  defp rank_pairs([k, v | rest]) when is_binary(k),
    do: [{k, to_float(v)} | rank_pairs(rest)]

  defp rank_pairs(_other), do: []

  defp to_float(n) when is_float(n), do: n
  defp to_float(n) when is_integer(n), do: n * 1.0
  defp to_float(nil), do: 0.0
end
