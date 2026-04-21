defmodule Aerospike.TelemetryTest do
  use ExUnit.Case, async: false

  alias Aerospike.Telemetry

  @span_events [
    {:pool_checkout_span, [:aerospike, :pool, :checkout]},
    {:command_send_span, [:aerospike, :command, :send]},
    {:command_recv_span, [:aerospike, :command, :recv]},
    {:info_rpc_span, [:aerospike, :info, :rpc]},
    {:tend_cycle_span, [:aerospike, :tender, :tend_cycle]},
    {:partition_map_refresh_span, [:aerospike, :tender, :partition_map_refresh]}
  ]

  @instant_events [
    {:node_transition, [:aerospike, :node, :transition]},
    {:retry_attempt, [:aerospike, :retry, :attempt]}
  ]

  def forward(event, measurements, metadata, test_pid) do
    send(test_pid, {:event, event, measurements, metadata})
  end

  describe "event-name constants" do
    test "span helpers return list-of-atoms prefixes" do
      for {fun, expected} <- @span_events do
        assert apply(Telemetry, fun, []) == expected
      end
    end

    test "instant helpers return full list-of-atoms event names" do
      for {fun, expected} <- @instant_events do
        assert apply(Telemetry, fun, []) == expected
      end
    end

    test "all event names live under [:aerospike, ...]" do
      for {fun, _} <- @span_events ++ @instant_events do
        assert [:aerospike | _] = apply(Telemetry, fun, [])
      end
    end

    test "helper lists expand the same taxonomy without drift" do
      expected_span_prefixes = Enum.map(@span_events, &elem(&1, 1))
      expected_instant_events = Enum.map(@instant_events, &elem(&1, 1))

      [pool_prefix | other_prefixes] = expected_span_prefixes

      expected_handler_events =
        Enum.map([:start, :stop], &(pool_prefix ++ [&1])) ++
          Enum.flat_map(other_prefixes, fn prefix ->
            Enum.map([:start, :stop, :exception], &(prefix ++ [&1]))
          end) ++ expected_instant_events

      assert Telemetry.span_prefixes() == expected_span_prefixes
      assert Telemetry.instant_event_names() == expected_instant_events
      assert Telemetry.handler_events() == expected_handler_events
    end
  end

  describe "dispatchability" do
    test "span prefixes drive :telemetry.span/3 end-to-end" do
      for {fun, prefix} <- @span_events do
        handler_id = {__MODULE__, fun}
        events = Enum.map([:start, :stop, :exception], &(prefix ++ [&1]))
        :ok = :telemetry.attach_many(handler_id, events, &__MODULE__.forward/4, self())

        :telemetry.span(apply(Telemetry, fun, []), %{probe: fun}, fn ->
          {:ok, %{probe: fun}}
        end)

        assert_receive {:event, event, %{monotonic_time: _}, %{probe: ^fun}}
        assert event == prefix ++ [:start]

        assert_receive {:event, event, %{duration: _}, %{probe: ^fun}}
        assert event == prefix ++ [:stop]

        :telemetry.detach(handler_id)
      end
    end

    test "instant event names fire via :telemetry.execute/3" do
      for {fun, event} <- @instant_events do
        handler_id = {__MODULE__, fun}
        :ok = :telemetry.attach(handler_id, event, &__MODULE__.forward/4, self())

        :telemetry.execute(apply(Telemetry, fun, []), %{count: 1}, %{probe: fun})

        assert_receive {:event, ^event, %{count: 1}, %{probe: ^fun}}

        :telemetry.detach(handler_id)
      end
    end
  end
end
