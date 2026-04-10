defmodule AeroMarketLiveWeb.DashboardLive do
  use AeroMarketLiveWeb, :live_view

  require Logger

  alias AeroMarketLive.Aero
  alias AeroMarketLive.MarketReader
  alias AeroMarketLive.TickerConfig
  alias Aerospike.Op.Map, as: MapOp

  # Must match the Generator's @window_size so the ring buffer holds exactly
  # what Aerospike retains — no more, no less. A refresh then recovers the
  # same window and the sparklines stay consistent across tabs.
  @ring_size 120

  # How long to wait between summary refreshes (ms).
  @summary_interval_ms 1_000

  @symbols Enum.map(TickerConfig.defaults(), & &1.symbol)

  # ---------------------------------------------------------------------------
  # Lifecycle
  # ---------------------------------------------------------------------------

  @impl true
  def mount(_params, _session, socket) do
    if connected?(socket) do
      Phoenix.PubSub.subscribe(AeroMarketLive.PubSub, "ticks")
      Process.send_after(self(), :refresh_summary, @summary_interval_ms)
    end

    empty_bufs = Map.new(@symbols, fn sym -> {sym, []} end)

    socket =
      assign(socket,
        symbols: @symbols,
        tick_bufs: empty_bufs,
        sparklines: Map.new(@symbols, fn sym -> {sym, sparkline_svg([])} end),
        current_prices: Map.new(@symbols, fn sym -> {sym, nil} end),
        summaries: Map.new(@symbols, fn sym -> {sym, :empty} end),
        leaderboard: [],
        today_ms: nil,
        summary_key: nil
      )

    {:ok, socket}
  end

  # ---------------------------------------------------------------------------
  # Message handlers
  # ---------------------------------------------------------------------------

  @impl true
  def handle_info({:tick, sym, day_ms, _index, price}, socket)
      when is_map_key(socket.assigns.tick_bufs, sym) do
    # When the simulated day rolls over, reset all ring buffers so the
    # sparklines don't blend two days. Also update today_ms and summary_key
    # so summary reads target the correct Aerospike key.
    socket =
      if day_ms != socket.assigns.today_ms do
        # First tick of a new (or the first) simulated day: seed ring buffers
        # from Aerospike so all tabs start from the same historical window.
        seeded_bufs = fetch_initial_ticks(Aero.conn(), @symbols, day_ms)

        assign(socket,
          today_ms: day_ms,
          summary_key: MarketReader.summary_key(day_ms),
          tick_bufs: seeded_bufs,
          sparklines: Map.new(seeded_bufs, fn {s, buf} -> {s, sparkline_svg(buf)} end),
          summaries: Map.new(@symbols, fn s -> {s, :empty} end),
          leaderboard: []
        )
      else
        socket
      end

    updated_buf =
      socket.assigns.tick_bufs
      |> Map.fetch!(sym)
      |> prepend_tick(price)

    {:noreply,
     assign(socket,
       tick_bufs: Map.put(socket.assigns.tick_bufs, sym, updated_buf),
       sparklines: Map.put(socket.assigns.sparklines, sym, sparkline_svg(updated_buf)),
       current_prices: Map.put(socket.assigns.current_prices, sym, price)
     )}
  end

  def handle_info({:tick, _sym, _day_ms, _index, _price}, socket) do
    {:noreply, socket}
  end

  def handle_info(:refresh_summary, %{assigns: %{today_ms: nil}} = socket) do
    Process.send_after(self(), :refresh_summary, @summary_interval_ms)
    {:noreply, socket}
  end

  def handle_info(:refresh_summary, socket) do
    conn = Aero.conn()
    today_ms = socket.assigns.today_ms
    summary_key = socket.assigns.summary_key

    summaries =
      Map.new(@symbols, fn sym ->
        result = MarketReader.fetch_day(conn, sym, today_ms)

        if match?({:ok, _}, result) do
          {:ok, day} = result
          MarketReader.write_overall_diff(conn, summary_key, sym, day)
        end

        {sym, result}
      end)

    leaderboard = MarketReader.fetch_leaderboard(conn, summary_key, length(@symbols))

    Process.send_after(self(), :refresh_summary, @summary_interval_ms)

    {:noreply, assign(socket, summaries: summaries, leaderboard: leaderboard)}
  end

  # ---------------------------------------------------------------------------
  # Private helpers
  # ---------------------------------------------------------------------------

  @namespace "test"
  @timeseries_set "timeseries"

  defp fetch_initial_ticks(conn, symbols, day_ms) do
    Map.new(symbols, fn sym ->
      key = Aerospike.key(@namespace, @timeseries_set, "#{sym}#{day_ms}")
      ops = [MapOp.get_by_index_range("stock", -@ring_size, @ring_size)]

      prices =
        case Aerospike.operate(conn, key, ops) do
          {:ok, record} -> kv_to_prices(record.bins["stock"])
          {:error, _} -> []
        end

      {sym, prices}
    end)
  end

  # CDT map results decode as %{minute_index => price}. Sort ascending by key
  # (oldest first), extract values, then reverse to newest-first for the buffer.
  defp kv_to_prices(nil), do: []

  defp kv_to_prices(m) when is_map(m) do
    m |> Enum.sort_by(fn {k, _} -> k end) |> Enum.map(fn {_, v} -> v end) |> Enum.reverse()
  end

  defp prepend_tick(buf, price), do: [price | Enum.take(buf, @ring_size - 1)]

  # ---------------------------------------------------------------------------
  # Render
  # ---------------------------------------------------------------------------

  @impl true
  def render(assigns) do
    ~H"""
    <div class="dashboard">
      <h1 class="dashboard-title">AeroMarketLive</h1>
      <table class="ticker-table">
        <thead>
          <tr>
            <th>Symbol</th>
            <th>Price</th>
            <th>Change</th>
            <th>Sparkline</th>
            <th>Max</th>
            <th>Min</th>
            <th>Avg</th>
          </tr>
        </thead>
        <tbody>
          <%= for sym <- @symbols do %>
            <tr class={ticker_row_class(@summaries[sym])}>
              <td class="sym">{sym}</td>
              {render_ticker_cells(assigns, sym)}
            </tr>
          <% end %>
        </tbody>
      </table>

      <section class="leaderboard">
        <h2>Top Performers</h2>
        <%= if @leaderboard == [] do %>
          <p class="empty">Loading…</p>
        <% else %>
          <ol>
            <%= for {ticker, diff} <- @leaderboard do %>
              <li class={delta_class(diff)}>
                <span class="lb-sym">{ticker}</span>
                <span class="lb-diff">{format_delta(diff)}</span>
              </li>
            <% end %>
          </ol>
        <% end %>
      </section>
    </div>
    """
  end

  # ---------------------------------------------------------------------------
  # Private render helpers
  # ---------------------------------------------------------------------------

  defp render_ticker_cells(assigns, sym) do
    summary = assigns.summaries[sym]
    price = assigns.current_prices[sym]
    sparkline = assigns.sparklines[sym]

    case summary do
      {:ok, day} ->
        change = (day.end_val - day.start_val) / day.start_val * 100.0

        assigns =
          assign(assigns,
            sym: sym,
            price: price || day.end_val,
            change: change,
            sparkline: sparkline,
            max: day.max,
            min: day.min,
            avg: day.avg
          )

        ~H"""
        <td class="num price">{fmt_price(@price)}</td>
        <td class={["num", "delta", delta_class(@change)]}>{format_pct(@change)}</td>
        <td class="sparkline">{raw(@sparkline)}</td>
        <td class="num">{fmt_price(@max)}</td>
        <td class="num">{fmt_price(@min)}</td>
        <td class="num">{fmt_price(@avg)}</td>
        """

      _ ->
        assigns = assign(assigns, sym: sym, price: price, sparkline: sparkline)

        ~H"""
        <td class="num price">{fmt_price(@price)}</td>
        <td class="num delta">—</td>
        <td class="sparkline">{raw(@sparkline)}</td>
        <td class="num">—</td>
        <td class="num">—</td>
        <td class="num">—</td>
        """
    end
  end

  defp sparkline_svg([]), do: ~s|<svg class="spark" viewBox="0 0 100 30"></svg>|

  defp sparkline_svg(buf) do
    # buf is newest-first; reverse to get chronological order for the chart
    prices = Enum.reverse(buf)
    n = length(prices)
    lo = Enum.min(prices)
    hi = Enum.max(prices)
    spread = hi - lo

    points =
      prices
      |> Enum.with_index()
      |> Enum.map(fn {p, i} ->
        x = if n > 1, do: i / (n - 1) * 100.0, else: 50.0
        y = if spread > 0, do: (1.0 - (p - lo) / spread) * 28.0 + 1.0, else: 15.0
        "#{Float.round(x, 2)},#{Float.round(y, 2)}"
      end)
      |> Enum.join(" ")

    ~s|<svg class="spark" viewBox="0 0 100 30" preserveAspectRatio="none"><polyline points="#{points}" /></svg>|
  end

  defp fmt_price(nil), do: "—"
  defp fmt_price(p) when is_float(p), do: :erlang.float_to_binary(p, decimals: 2)
  defp fmt_price(p) when is_integer(p), do: "#{p}.00"

  defp format_pct(pct) when is_float(pct) do
    sign = if pct >= 0, do: "+", else: ""
    "#{sign}#{:erlang.float_to_binary(pct, decimals: 2)}%"
  end

  defp format_delta(diff) when is_float(diff) do
    sign = if diff >= 0, do: "+", else: ""
    "#{sign}#{:erlang.float_to_binary(diff, decimals: 2)}"
  end

  defp ticker_row_class({:ok, %{is_positive: true}}), do: "ticker-row positive-day"
  defp ticker_row_class({:ok, %{is_positive: false}}), do: "ticker-row negative-day"
  defp ticker_row_class(_summary), do: "ticker-row"

  defp delta_class(n) when n >= 0, do: "up"
  defp delta_class(_n), do: "down"
end
