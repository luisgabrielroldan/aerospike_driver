defmodule Aerospike.TenderOrchestrationTest do
  @moduledoc """
  Orchestration-level seam test. Drives one full tend cycle against a
  scripted `Aerospike.Transport.Fake` and asserts the three observable
  telemetry signals operators rely on: the tend-cycle span, the nested
  partition-map refresh span, and the node-transition event. This test
  intentionally observes only telemetry — not internal Tender state — so
  it keeps passing across future refactors that preserve the same
  observable surface.
  """

  use ExUnit.Case, async: true

  alias Aerospike.PartitionMapWriter
  alias Aerospike.TableOwner
  alias Aerospike.Telemetry
  alias Aerospike.Tender
  alias Aerospike.Test.ReplicasFixture
  alias Aerospike.Transport.Fake

  def forward(event, measurements, metadata, test_pid) do
    send(test_pid, {:event, event, measurements, metadata})
  end

  setup context do
    name = :"orchestration_#{:erlang.phash2(context.test)}"

    {:ok, fake} = Fake.start_link(nodes: [{"A1", "10.0.0.1", 3000}])
    {:ok, owner} = TableOwner.start_link(name: name)
    tables = TableOwner.tables(owner)
    {:ok, writer} = PartitionMapWriter.start_link(name: name, tables: tables)

    on_exit(fn ->
      stop_if_alive(fake)
      stop_if_alive(writer)
      stop_if_alive(owner)
    end)

    %{name: name, fake: fake, owner: owner, writer: writer, tables: tables}
  end

  test "a healthy tend cycle emits tend_cycle, partition_map_refresh, and node_transition",
       ctx do
    script_bootstrap_node(ctx.fake, "A1", 1, ReplicasFixture.all_master("test", 1))

    events = [
      Telemetry.tend_cycle_span() ++ [:start],
      Telemetry.tend_cycle_span() ++ [:stop],
      Telemetry.partition_map_refresh_span() ++ [:start],
      Telemetry.partition_map_refresh_span() ++ [:stop],
      Telemetry.node_transition()
    ]

    handler_id = attach_handler(events)
    on_exit(fn -> :telemetry.detach(handler_id) end)

    {:ok, pid} = start_tender(ctx, "test")

    :ok = Tender.tend_now(pid)

    # Cycle span — start before, stop after.
    assert_receive {:event, [:aerospike, :tender, :tend_cycle, :start], _, _}, 1_000

    # Nested partition-map refresh span sits inside the cycle.
    assert_receive {:event, [:aerospike, :tender, :partition_map_refresh, :start], _, _},
                   1_000

    assert_receive {:event, [:aerospike, :tender, :partition_map_refresh, :stop], _, _},
                   1_000

    # Node transition fires when the seed registers (unknown -> active).
    assert_receive {:event, [:aerospike, :node, :transition], _, metadata}, 1_000
    assert %{node_name: "A1", from: :unknown, to: :active} = metadata

    # Cycle span closes after the inner span has stopped.
    assert_receive {:event, [:aerospike, :tender, :tend_cycle, :stop], stop_meas, _}, 1_000
    assert %{duration: duration} = stop_meas
    assert is_integer(duration) and duration >= 0

    assert Tender.ready?(pid)
  end

  ## Helpers

  defp start_tender(ctx, namespace) do
    tender_opts = [
      name: ctx.name,
      transport: Fake,
      connect_opts: [fake: ctx.fake],
      seeds: [{"10.0.0.1", 3000}],
      namespaces: [namespace],
      tables: ctx.tables,
      tend_trigger: :manual
    ]

    {:ok, pid} = Tender.start_link(tender_opts)
    on_exit(fn -> stop_tender(pid) end)
    {:ok, pid}
  end

  defp script_bootstrap_node(fake, node_name, partition_gen, replicas_value) do
    Fake.script_info(fake, node_name, ["node", "features"], %{
      "node" => node_name,
      "features" => ""
    })

    Fake.script_info(
      fake,
      node_name,
      ["partition-generation", "cluster-stable", "peers-generation"],
      %{
        "partition-generation" => Integer.to_string(partition_gen),
        "cluster-stable" => "deadbeef",
        "peers-generation" => "1"
      }
    )

    Fake.script_info(fake, node_name, ["peers-clear-std"], %{
      "peers-clear-std" => "0,3000,[]"
    })

    Fake.script_info(fake, node_name, ["replicas"], %{"replicas" => replicas_value})
  end

  defp attach_handler(events) do
    handler_id = {__MODULE__, make_ref()}
    :ok = :telemetry.attach_many(handler_id, events, &__MODULE__.forward/4, self())
    handler_id
  end

  defp stop_tender(pid) do
    if Process.alive?(pid) do
      ref = Process.monitor(pid)

      try do
        GenServer.stop(pid, :normal, 2_000)
      catch
        :exit, _ -> :ok
      end

      receive do
        {:DOWN, ^ref, _, _, _} -> :ok
      after
        2_000 -> :ok
      end
    end
  end

  defp stop_if_alive(pid) when is_pid(pid) do
    if Process.alive?(pid) do
      ref = Process.monitor(pid)

      try do
        GenServer.stop(pid, :normal, 2_000)
      catch
        :exit, _ -> :ok
      end

      receive do
        {:DOWN, ^ref, _, _, _} -> :ok
      after
        2_000 -> :ok
      end
    end
  end

  defp stop_if_alive(_), do: :ok
end
