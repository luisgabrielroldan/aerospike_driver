defmodule Aerospike.TelemetryTest do
  use ExUnit.Case, async: false

  alias Aerospike.Cluster.TableOwner
  alias Aerospike.Error
  alias Aerospike.RuntimeMetrics
  alias Aerospike.Telemetry

  @span_events [
    {:pool_checkout_span, [:aerospike, :pool, :checkout],
     "[:aerospike, :pool, :checkout, :start | :stop | :exception]"},
    {:command_send_span, [:aerospike, :command, :send],
     "[:aerospike, :command, :send, :start | :stop | :exception]"},
    {:command_recv_span, [:aerospike, :command, :recv],
     "[:aerospike, :command, :recv, :start | :stop | :exception]"},
    {:info_rpc_span, [:aerospike, :info, :rpc],
     "[:aerospike, :info, :rpc, :start | :stop | :exception]"},
    {:tend_cycle_span, [:aerospike, :tender, :tend_cycle],
     "[:aerospike, :tender, :tend_cycle, :start | :stop | :exception]"},
    {:partition_map_refresh_span, [:aerospike, :tender, :partition_map_refresh],
     "[:aerospike, :tender, :partition_map_refresh, :start | :stop | :exception]"}
  ]

  @instant_events [
    {:node_transition, [:aerospike, :node, :transition], "[:aerospike, :node, :transition]"},
    {:retry_attempt, [:aerospike, :retry, :attempt], "[:aerospike, :retry, :attempt]"}
  ]

  @telemetry_doc Path.expand("../../../spike-docs/telemetry.md", __DIR__)
  @telemetry_guide Path.expand("../../guides/telemetry-and-runtime-metrics.md", __DIR__)
  @doc_emitters [
    "Aerospike.Runtime.PoolCheckout",
    "Aerospike.Transport.Tcp",
    "Aerospike.Cluster.Tender",
    "Aerospike.Telemetry.emit_retry_attempt/4"
  ]

  def forward(event, measurements, metadata, test_pid) do
    send(test_pid, {:event, event, measurements, metadata})
  end

  describe "event-name constants" do
    test "span helpers return list-of-atoms prefixes" do
      for {fun, expected, _doc_heading} <- @span_events do
        assert apply(Telemetry, fun, []) == expected
      end
    end

    test "instant helpers return full list-of-atoms event names" do
      for {fun, expected, _doc_heading} <- @instant_events do
        assert apply(Telemetry, fun, []) == expected
      end
    end

    test "all event names live under [:aerospike, ...]" do
      for {fun, _expected, _doc_heading} <- @span_events ++ @instant_events do
        assert [:aerospike | _] = apply(Telemetry, fun, [])
      end
    end

    test "helper lists expand the same taxonomy without drift" do
      expected_span_prefixes = Enum.map(@span_events, &span_prefix/1)
      expected_instant_events = Enum.map(@instant_events, &instant_event_name/1)

      expected_handler_events =
        Enum.flat_map(expected_span_prefixes, fn prefix ->
          Enum.map([:start, :stop, :exception], &(prefix ++ [&1]))
        end) ++ expected_instant_events

      assert Telemetry.span_prefixes() == expected_span_prefixes
      assert Telemetry.instant_event_names() == expected_instant_events
      assert Telemetry.handler_events() == expected_handler_events
    end

    test "telemetry doc headings match the supported taxonomy" do
      assert documented_event_headings() == expected_doc_headings()
    end

    test "telemetry doc still names the current emitters" do
      doc = File.read!(@telemetry_doc)

      for emitter <- @doc_emitters do
        assert doc =~ emitter
      end
    end

    test "published telemetry guide names the supported taxonomy" do
      guide = File.read!(@telemetry_guide)

      for {_fun, _event, doc_heading} <- @span_events ++ @instant_events do
        assert guide =~ doc_heading
      end

      assert guide =~ "Aerospike.Telemetry.handler_events()"
    end
  end

  describe "dispatchability" do
    test "span prefixes drive :telemetry.span/3 end-to-end" do
      for {fun, prefix, _doc_heading} <- @span_events do
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
      for {fun, event, _doc_heading} <- @instant_events do
        handler_id = {__MODULE__, fun}
        :ok = :telemetry.attach(handler_id, event, &__MODULE__.forward/4, self())

        :telemetry.execute(apply(Telemetry, fun, []), %{count: 1}, %{probe: fun})

        assert_receive {:event, ^event, %{count: 1}, %{probe: ^fun}}

        :telemetry.detach(handler_id)
      end
    end
  end

  describe "runtime metrics collector" do
    test "tracks cluster-level command and retry counters" do
      name = :"metrics_#{System.unique_integer([:positive])}"
      {:ok, owner} = TableOwner.start_link(name: name)

      on_exit(fn ->
        if Process.alive?(owner) do
          GenServer.stop(owner)
        end
      end)

      :ok = RuntimeMetrics.init(name, pool_size: 4, tend_interval_ms: 1_500)
      refute RuntimeMetrics.metrics_enabled?(name)

      assert :ok = RuntimeMetrics.enable(name, reset: true)

      start_mono = System.monotonic_time() - System.convert_time_unit(5, :millisecond, :native)
      :ok = RuntimeMetrics.record_command(name, Aerospike.Command.Get, start_mono, {:ok, :value})

      :ok =
        RuntimeMetrics.record_command(
          name,
          Aerospike.Command.Batch,
          start_mono,
          {:error, Error.from_result_code(:timeout)}
        )

      :ok = RuntimeMetrics.record_retry_attempt(name, :transport)

      stats = RuntimeMetrics.stats(name)

      assert stats.metrics_enabled
      assert stats.cluster.config == %{pool_size: 4, tend_interval_ms: 1_500}
      assert stats.commands_total == 2
      assert stats.commands_ok == 1
      assert stats.commands_error == 1
      assert stats.cluster.retries.transport == 1
      assert stats.cluster.commands.by_command[Aerospike.Command.Get].ok == 1
      assert stats.cluster.commands.by_command[Aerospike.Command.Batch].error == 1
      assert stats.cluster.commands.errors_by_code[:timeout] == 1
    end
  end

  defp expected_doc_headings do
    Enum.map(@span_events ++ @instant_events, &doc_heading/1)
  end

  defp documented_event_headings do
    @telemetry_doc
    |> File.read!()
    |> String.split("\n## ", trim: true)
    |> Enum.drop(1)
    |> Enum.map(fn section ->
      section
      |> String.split("\n", parts: 2)
      |> hd()
      |> String.trim()
      |> String.trim_leading("`")
      |> String.trim_trailing("`")
    end)
  end

  defp span_prefix({_fun, prefix, _doc_heading}), do: prefix
  defp instant_event_name({_fun, event, _doc_heading}), do: event
  defp doc_heading({_fun, _event, doc_heading}), do: doc_heading
end
