defmodule Aerospike.Cluster.TenderSeedRebootstrapTest do
  use ExUnit.Case, async: true

  alias Aerospike.Cluster.PartitionMapWriter
  alias Aerospike.Cluster.TableOwner
  alias Aerospike.Cluster.Tender
  alias Aerospike.Test.ReplicasFixture
  alias Aerospike.Transport.Fake

  @namespace "test"
  @seed_a {"10.0.0.1", 3000}
  @seed_b {"10.0.0.2", 3000}

  setup context do
    name = :"seed_rebootstrap_#{:erlang.phash2(context.test)}"

    {:ok, fake} =
      Fake.start_link(
        nodes: [
          {"A1", elem(@seed_a, 0), elem(@seed_a, 1)},
          {"B1", elem(@seed_b, 0), elem(@seed_b, 1)}
        ]
      )

    {:ok, owner} = TableOwner.start_link(name: name)
    tables = TableOwner.tables(owner)
    {:ok, writer} = PartitionMapWriter.start_link(name: name, tables: tables)

    on_exit(fn ->
      stop_if_alive(fake)
      stop_if_alive(writer)
      stop_if_alive(owner)
    end)

    %{name: name, fake: fake, tables: tables}
  end

  test "re-bootstraps a configured seed when it returns under a new node id", ctx do
    script_seed_bootstrap(ctx.fake, "A1")
    script_seed_bootstrap(ctx.fake, "B1")
    script_refresh_cycle(ctx.fake, "A1", 1)
    script_refresh_cycle(ctx.fake, "B1", 1)

    # B1 stays healthy while A1 is disconnected and later replaced.
    script_refresh_cycle(ctx.fake, "B1", 1)
    script_refresh_only(ctx.fake, "B1", 1)
    script_refresh_only(ctx.fake, "B1", 1)

    {:ok, tender} = start_tender(ctx)

    :ok = Tender.tend_now(tender)

    assert %{"A1" => %{status: :active}, "B1" => %{status: :active}} = Tender.nodes_status(tender)

    Fake.disconnect(ctx.fake, "A1")

    # failure_threshold = 1:
    # cycle 2 => A1 active -> inactive
    # cycle 3 => A1 inactive -> dropped
    :ok = Tender.tend_now(tender)
    :ok = Tender.tend_now(tender)

    refute Map.has_key?(Tender.nodes_status(tender), "A1")
    assert %{"B1" => %{status: :active}} = Tender.nodes_status(tender)

    Fake.register_node(ctx.fake, "A2", elem(@seed_a, 0), elem(@seed_a, 1))
    script_seed_bootstrap(ctx.fake, "A2")
    script_refresh_cycle(ctx.fake, "A2", 7)

    :ok = Tender.tend_now(tender)

    assert %{
             "A2" => %{status: :active, generation_seen: 7},
             "B1" => %{status: :active}
           } = Tender.nodes_status(tender)
  end

  defp start_tender(ctx) do
    tender_opts = [
      name: ctx.name,
      transport: Fake,
      connect_opts: [fake: ctx.fake],
      seeds: [@seed_a, @seed_b],
      namespaces: [@namespace],
      tables: ctx.tables,
      tend_trigger: :manual,
      failure_threshold: 1
    ]

    {:ok, pid} = Tender.start_link(tender_opts)
    on_exit(fn -> stop_if_alive(pid) end)
    {:ok, pid}
  end

  defp script_seed_bootstrap(fake, node_name) do
    Fake.script_info(fake, node_name, ["node", "features"], %{
      "node" => node_name,
      "features" => ""
    })
  end

  defp script_refresh_cycle(fake, node_name, partition_gen) do
    script_refresh_only(fake, node_name, partition_gen)

    Fake.script_info(fake, node_name, ["peers-clear-std"], %{
      "peers-clear-std" => "0,3000,[]"
    })

    Fake.script_info(fake, node_name, ["replicas"], %{
      "replicas" => ReplicasFixture.all_master(@namespace, 1)
    })
  end

  defp script_refresh_only(fake, node_name, partition_gen) do
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
