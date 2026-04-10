defmodule AeroMarketLive.Generator do
  @moduledoc """
  Synthetic market-data generator.

  A single GenServer drives every ticker in `AeroMarketLive.TickerConfig.defaults/0`.
  On each wall-clock tick it advances a simulated clock by `minutes_per_tick`
  simulated minutes, draws a geometric Brownian motion step for every symbol,
  writes the new price into the `test.timeseries` set using a CDT map shape
  (`stock` ordered map + `sum` running total), and broadcasts a `{:tick, ...}`
  event on the `"ticks"` PubSub topic.

  The two update paths into the dashboard are deliberately independent:

    * High-frequency tick events flow through `Phoenix.PubSub` and feed
      sparklines without an Aerospike round-trip.
    * The LiveView's periodic summary refresh calls `MarketReader` against
      the same records this generator writes.

  A single-in-flight cadence is guaranteed by scheduling the next `:tick`
  at the **end** of each handler, not at fixed intervals.
  """

  use GenServer

  require Logger

  alias Aerospike.Op
  alias Aerospike.Op.Map, as: MapOp
  alias AeroMarketLive.Aero
  alias AeroMarketLive.DateHelper
  alias AeroMarketLive.TickerConfig

  @namespace "test"
  @set "timeseries"
  @pubsub AeroMarketLive.PubSub
  @topic "ticks"

  # KEY_VALUE_ORDERED (3), no write-flag constraints.
  @map_policy %{attr: 3, flags: 0}

  # 6.5-hour US equity trading session.
  @minutes_per_day 390
  @window_size 120

  # Deterministic seed so repeated runs produce the same price trajectory.
  @rand_seed {1_618_033, 2_718_281, 3_141_592}

  @default_tick_ms 1_000
  @default_minutes_per_tick 12

  defstruct [
    :tick_ms,
    :minutes_per_tick,
    :dt,
    :sim_time_ms,
    :tickers,
    :rand_state,
    :paused
  ]

  @type ticker_state :: %{
          price: float(),
          drift: float(),
          vol: float(),
          day_ms: integer(),
          minute_index: non_neg_integer()
        }

  @type t :: %__MODULE__{
          tick_ms: pos_integer(),
          minutes_per_tick: pos_integer(),
          dt: float(),
          sim_time_ms: integer(),
          tickers: %{String.t() => ticker_state()},
          rand_state: :rand.state(),
          paused: boolean()
        }

  # ---------------------------------------------------------------------------
  # Public API
  # ---------------------------------------------------------------------------

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc "Returns the full generator state — intended for iex inspection."
  @spec state() :: t()
  def state, do: GenServer.call(__MODULE__, :state)

  @doc "Stops further ticking until `resume/0` is called."
  @spec pause() :: :ok
  def pause, do: GenServer.call(__MODULE__, :pause)

  @doc "Resumes ticking after a `pause/0`."
  @spec resume() :: :ok
  def resume, do: GenServer.call(__MODULE__, :resume)

  # ---------------------------------------------------------------------------
  # GenServer callbacks
  # ---------------------------------------------------------------------------

  @impl true
  def init(opts) do
    tick_ms = Keyword.get(opts, :tick_ms, @default_tick_ms)
    minutes_per_tick = Keyword.get(opts, :minutes_per_tick, @default_minutes_per_tick)
    configs = Keyword.get(opts, :tickers, TickerConfig.defaults())

    now_ms = System.system_time(:millisecond)
    day_ms = DateHelper.midnight_ms(now_ms)

    state = %__MODULE__{
      tick_ms: tick_ms,
      minutes_per_tick: minutes_per_tick,
      dt: minutes_per_tick / (@minutes_per_day * 1.0),
      sim_time_ms: day_ms,
      tickers: init_tickers(configs, day_ms),
      rand_state: :rand.seed_s(:exsss, @rand_seed),
      paused: false
    }

    schedule_tick(tick_ms)
    {:ok, state}
  end

  @impl true
  def handle_call(:state, _from, state), do: {:reply, state, state}

  def handle_call(:pause, _from, state), do: {:reply, :ok, %{state | paused: true}}

  def handle_call(:resume, _from, state), do: {:reply, :ok, %{state | paused: false}}

  @impl true
  def handle_info(:tick, %__MODULE__{paused: true} = state) do
    schedule_tick(state.tick_ms)
    {:noreply, state}
  end

  def handle_info(:tick, %__MODULE__{} = state) do
    new_sim_time = state.sim_time_ms + state.minutes_per_tick * 60_000

    {new_tickers, rand_state} =
      advance_tickers(state.tickers, new_sim_time, state.dt, state.rand_state)

    new_state = %{state | sim_time_ms: new_sim_time, tickers: new_tickers, rand_state: rand_state}

    schedule_tick(state.tick_ms)
    {:noreply, new_state}
  end

  # ---------------------------------------------------------------------------
  # Internals
  # ---------------------------------------------------------------------------

  defp init_tickers(configs, day_ms) do
    Map.new(configs, fn %TickerConfig{} = cfg ->
      {cfg.symbol,
       %{
         price: cfg.initial_price,
         drift: cfg.drift,
         vol: cfg.volatility,
         day_ms: day_ms,
         minute_index: 0
       }}
    end)
  end

  defp advance_tickers(tickers, sim_time_ms, dt, rand_state) do
    new_day_ms = DateHelper.midnight_ms(sim_time_ms)

    Enum.reduce(tickers, {%{}, rand_state}, fn {sym, ticker}, {acc, rs} ->
      {next, rs2} = step_ticker(sym, ticker, new_day_ms, dt, rs)
      {Map.put(acc, sym, next), rs2}
    end)
  end

  defp step_ticker(sym, ticker, new_day_ms, dt, rand_state) do
    ticker = maybe_rollover(ticker, new_day_ms)

    if ticker.minute_index >= @minutes_per_day do
      {ticker, rand_state}
    else
      {gauss, rs} = box_muller(rand_state)

      next_price =
        ticker.price *
          :math.exp(ticker.drift * dt + ticker.vol * :math.sqrt(dt) * gauss)

      new_index = ticker.minute_index + 1

      write_tick(sym, ticker.day_ms, new_index, next_price)
      broadcast_tick(sym, ticker.day_ms, new_index, next_price)

      {%{ticker | price: next_price, minute_index: new_index}, rs}
    end
  end

  defp maybe_rollover(%{day_ms: day_ms} = ticker, new_day_ms) when new_day_ms == day_ms,
    do: ticker

  defp maybe_rollover(ticker, new_day_ms) do
    %{ticker | day_ms: new_day_ms, minute_index: 0}
  end

  defp write_tick(sym, day_ms, index, price) do
    key = Aerospike.key(@namespace, @set, "#{sym}#{day_ms}")

    ops = [
      MapOp.put("stock", index, price, policy: @map_policy),
      Op.add("sum", price),
      MapOp.remove_by_index_range("stock", 0, trim_count(index))
    ]

    case Aerospike.operate(Aero.conn(), key, ops) do
      {:ok, _record} ->
        :ok

      {:error, reason} ->
        Logger.warning("[Generator] write failed for #{sym}@#{index}: #{inspect(reason)}")
        :ok
    end
  end

  defp broadcast_tick(sym, day_ms, index, price) do
    Phoenix.PubSub.broadcast(@pubsub, @topic, {:tick, sym, day_ms, index, price})
  end

  # Box–Muller transform: two uniform draws produce one standard normal.
  # rand_state is threaded explicitly so the deterministic seed is preserved.
  defp box_muller(rand_state) do
    {u1, rs1} = :rand.uniform_real_s(rand_state)
    {u2, rs2} = :rand.uniform_real_s(rs1)
    z = :math.sqrt(-2.0 * :math.log(u1)) * :math.cos(2.0 * :math.pi() * u2)
    {z, rs2}
  end

  defp trim_count(index) when index > @window_size, do: 1
  defp trim_count(_index), do: 0

  defp schedule_tick(ms), do: Process.send_after(self(), :tick, ms)
end
