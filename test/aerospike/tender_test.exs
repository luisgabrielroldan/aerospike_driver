defmodule Aerospike.TenderTest do
  use ExUnit.Case, async: true

  alias Aerospike.Error
  alias Aerospike.PartitionMap
  alias Aerospike.TableOwner
  alias Aerospike.Tender
  alias Aerospike.Test.ReplicasFixture
  alias Aerospike.Transport.Fake

  setup context do
    name = :"tender_#{:erlang.phash2(context.test)}"

    {:ok, fake} =
      Fake.start_link(nodes: [{"A1", "10.0.0.1", 3000}])

    {:ok, owner} = TableOwner.start_link(name: name)
    tables = TableOwner.tables(owner)

    on_exit(fn ->
      stop_fake(fake)
      stop_process(owner)
    end)

    %{name: name, fake: fake, owner: owner, tables: tables}
  end

  describe "single-seed bootstrap" do
    test "one tend cycle sets :ready and fills every partition", ctx do
      script_bootstrap_node(ctx.fake, "A1", 1, ReplicasFixture.all_master("test", 1))

      {:ok, pid} = start_tender(ctx, "test")

      refute Tender.ready?(pid)

      :ok = Tender.tend_now(pid)

      assert Tender.ready?(pid)

      %{owners: owners} = Tender.tables(pid)

      for partition_id <- 0..(PartitionMap.partition_count() - 1) do
        {:ok, po} = PartitionMap.owners(owners, "test", partition_id)
        assert po.regime == 1
        assert po.replicas == ["A1"]
      end
    end
  end

  describe "tend_now/1 synchronisation" do
    test "returns only after the cycle finishes writing ETS", ctx do
      script_bootstrap_node(ctx.fake, "A1", 3, ReplicasFixture.all_master("test", 3))

      {:ok, pid} = start_tender(ctx, "test")

      :ok = Tender.tend_now(pid)

      %{node_gens: node_gens, owners: owners} = Tender.tables(pid)

      assert {:ok, 3} = PartitionMap.get_node_gen(node_gens, "A1")
      assert {:ok, _po} = PartitionMap.owners(owners, "test", 0)
      assert Tender.ready?(pid)
    end
  end

  describe "failure threshold: node stays present until N+1 consecutive failures" do
    test "node is dropped only once failures reach the threshold", ctx do
      # Threshold = 5. Each fully-failing cycle contributes 3 failure events
      # (partition-generation, peers-clear-std, replicas). Cycle 1 is healthy
      # (failures = 0). Cycle 2 fails every call (failures = 3, still below
      # the threshold, so the node survives). Cycle 3's second failure lifts
      # the counter to 5 and drops the node mid-cycle.
      script_bootstrap_node(ctx.fake, "A1", 2, ReplicasFixture.all_master("test", 2))

      err = %Error{code: :network_error, message: "injected"}

      # Two fully-failing cycles worth of scripted errors is enough; the third
      # cycle's replicas script goes unused because drop_node fires first.
      Enum.each(1..2, fn _ ->
        Fake.script_info_error(ctx.fake, "A1", ["partition-generation"], err)
        Fake.script_info_error(ctx.fake, "A1", ["peers-clear-std"], err)
        Fake.script_info_error(ctx.fake, "A1", ["replicas"], err)
      end)

      {:ok, pid} = start_tender(ctx, "test", failure_threshold: 5)

      :ok = Tender.tend_now(pid)
      assert Tender.ready?(pid)

      %{owners: owners, node_gens: node_gens} = Tender.tables(pid)
      assert {:ok, _} = PartitionMap.get_node_gen(node_gens, "A1")

      # Cycle 2 — failures climb to 3 but stay below threshold.
      :ok = Tender.tend_now(pid)

      assert {:ok, _} = PartitionMap.get_node_gen(node_gens, "A1")
      {:ok, po} = PartitionMap.owners(owners, "test", 0)
      assert "A1" in po.replicas

      # Cycle 3 — threshold reached; node dropped and owners cleared.
      :ok = Tender.tend_now(pid)

      assert {:error, :unknown_node} = PartitionMap.get_node_gen(node_gens, "A1")
      assert {:error, :unknown_partition} = PartitionMap.owners(owners, "test", 0)
      refute Tender.ready?(pid)
    end
  end

  describe "regime guard: older regime replicas reply is ignored" do
    test "a lower-regime replicas reply does not overwrite a higher-regime entry", ctx do
      # First cycle installs regime 5. Second cycle pretends the node regressed
      # to regime 4 (as if a lagging partition-map was read after a newer one).
      # The stored entry must stay at regime 5.
      Fake.script_info(ctx.fake, "A1", ["node"], %{"node" => "A1"})

      Enum.each([5, 4], fn regime ->
        Fake.script_info(ctx.fake, "A1", ["partition-generation"], %{
          "partition-generation" => Integer.to_string(regime)
        })

        Fake.script_info(ctx.fake, "A1", ["peers-clear-std"], %{
          "peers-clear-std" => "0,3000,[]"
        })

        Fake.script_info(ctx.fake, "A1", ["replicas"], %{
          "replicas" => ReplicasFixture.all_master("test", regime)
        })
      end)

      {:ok, pid} = start_tender(ctx, "test")

      :ok = Tender.tend_now(pid)
      :ok = Tender.tend_now(pid)

      %{owners: owners} = Tender.tables(pid)

      for partition_id <- [0, 1234, PartitionMap.partition_count() - 1] do
        {:ok, po} = PartitionMap.owners(owners, "test", partition_id)
        assert po.regime == 5
        assert po.replicas == ["A1"]
      end
    end
  end

  describe "ready-means-routable: ready flag is false while any partition is unowned" do
    test "ready? stays false when the replicas reply covers only a subset of partitions", ctx do
      last_partition = PartitionMap.partition_count() - 1

      incomplete =
        ReplicasFixture.build("test", 1, [Enum.to_list(0..(last_partition - 1))])

      Fake.script_info(ctx.fake, "A1", ["node"], %{"node" => "A1"})

      Fake.script_info(ctx.fake, "A1", ["partition-generation"], %{
        "partition-generation" => "1"
      })

      Fake.script_info(ctx.fake, "A1", ["peers-clear-std"], %{
        "peers-clear-std" => "0,3000,[]"
      })

      Fake.script_info(ctx.fake, "A1", ["replicas"], %{"replicas" => incomplete})

      {:ok, pid} = start_tender(ctx, "test")

      :ok = Tender.tend_now(pid)

      %{owners: owners} = Tender.tables(pid)

      assert {:ok, _} = PartitionMap.owners(owners, "test", 0)
      assert {:error, :unknown_partition} = PartitionMap.owners(owners, "test", last_partition)

      refute Tender.ready?(pid)
    end
  end

  describe "iterate-until-advance: stale first-by-term-order peer does not mask topology" do
    test "empty peers reply from sorted-first node does not hide peers reported by later nodes",
         ctx do
      # A1 sorts first lexicographically. If peer discovery shortcut on A1's
      # empty reply, C1 (advertised by B1) would never be reached and the map
      # would stay incomplete. Iterate-until-advance unions all replies, so C1
      # joins on the second cycle and the cluster becomes ready.
      register_node(ctx.fake, "B1", "10.0.0.2", 3000)
      register_node(ctx.fake, "C1", "10.0.0.3", 3000)

      last_partition = PartitionMap.partition_count() - 1
      half = div(last_partition + 1, 2)

      a1_replicas =
        ReplicasFixture.build("test", 1, [Enum.to_list(0..(half - 1))])

      c1_replicas =
        ReplicasFixture.build("test", 1, [Enum.to_list(half..last_partition)])

      # Bootstrap: A1 and B1 are both seeds.
      Fake.script_info(ctx.fake, "A1", ["node"], %{"node" => "A1"})
      Fake.script_info(ctx.fake, "B1", ["node"], %{"node" => "B1"})

      # Cycle 1 — peers discovery returns empty from A1 and empty from B1.
      # Partition map is incomplete (half owned by A1, no one owns the rest).
      script_cycle(ctx.fake, "A1", gen: 1, peers: "0,3000,[]", replicas: a1_replicas)
      script_cycle(ctx.fake, "B1", gen: 1, peers: "0,3000,[]", replicas: empty_replicas())

      # Cycle 2 — A1 still reports empty peers; B1 now advertises C1.
      script_cycle(ctx.fake, "A1", gen: 1, peers: "0,3000,[]", replicas: a1_replicas)

      script_cycle(ctx.fake, "B1",
        gen: 1,
        peers: "1,3000,[[C1,,[10.0.0.3:3000]]]",
        replicas: empty_replicas()
      )

      # C1 gets reached inside cycle 2 via ensure_peer_connected + replicas.
      Fake.script_info(ctx.fake, "C1", ["replicas"], %{"replicas" => c1_replicas})

      {:ok, pid} =
        start_tender(ctx, "test", seeds: [{"10.0.0.1", 3000}, {"10.0.0.2", 3000}])

      :ok = Tender.tend_now(pid)
      refute Tender.ready?(pid)

      %{owners: owners} = Tender.tables(pid)
      assert {:error, :unknown_partition} = PartitionMap.owners(owners, "test", last_partition)

      :ok = Tender.tend_now(pid)

      assert Tender.ready?(pid)

      {:ok, po_first} = PartitionMap.owners(owners, "test", 0)
      assert "A1" in po_first.replicas

      {:ok, po_last} = PartitionMap.owners(owners, "test", last_partition)
      assert "C1" in po_last.replicas
    end
  end

  describe "partial tend: mid-cycle transport failure does not leave partial writes" do
    test "a node's replicas error does not corrupt another node's owners entries", ctx do
      register_node(ctx.fake, "B1", "10.0.0.2", 3000)

      last_partition = PartitionMap.partition_count() - 1
      half = div(last_partition + 1, 2)

      a1_replicas =
        ReplicasFixture.build("test", 1, [Enum.to_list(0..(half - 1))])

      b1_replicas =
        ReplicasFixture.build("test", 1, [Enum.to_list(half..last_partition)])

      Fake.script_info(ctx.fake, "A1", ["node"], %{"node" => "A1"})
      Fake.script_info(ctx.fake, "B1", ["node"], %{"node" => "B1"})

      # Cycle 1 — both nodes succeed; map is complete and ready.
      script_cycle(ctx.fake, "A1", gen: 1, peers: "0,3000,[]", replicas: a1_replicas)
      script_cycle(ctx.fake, "B1", gen: 1, peers: "0,3000,[]", replicas: b1_replicas)

      # Cycle 2 — A1 errors on `replicas`; B1 still replies successfully.
      Fake.script_info(ctx.fake, "A1", ["partition-generation"], %{"partition-generation" => "1"})
      Fake.script_info(ctx.fake, "A1", ["peers-clear-std"], %{"peers-clear-std" => "0,3000,[]"})

      Fake.script_info_error(
        ctx.fake,
        "A1",
        ["replicas"],
        %Error{code: :network_error, message: "injected"}
      )

      script_cycle(ctx.fake, "B1", gen: 1, peers: "0,3000,[]", replicas: b1_replicas)

      {:ok, pid} =
        start_tender(ctx, "test", seeds: [{"10.0.0.1", 3000}, {"10.0.0.2", 3000}])

      :ok = Tender.tend_now(pid)
      assert Tender.ready?(pid)

      %{owners: owners, node_gens: node_gens} = Tender.tables(pid)

      :ok = Tender.tend_now(pid)

      # A1 is still known — one failure is well below the threshold.
      assert {:ok, 1} = PartitionMap.get_node_gen(node_gens, "A1")
      assert {:ok, 1} = PartitionMap.get_node_gen(node_gens, "B1")

      # A1's previously-written partitions survive its mid-cycle error.
      {:ok, po_a} = PartitionMap.owners(owners, "test", 0)
      assert po_a.regime == 1
      assert "A1" in po_a.replicas

      # B1's partitions also remain intact.
      {:ok, po_b} = PartitionMap.owners(owners, "test", last_partition)
      assert po_b.regime == 1
      assert "B1" in po_b.replicas

      assert Tender.ready?(pid)
    end
  end

  describe "TableOwner outlives the Tender: restart preserves ETS state" do
    test "a restarted Tender reuses the same tables with their prior contents", ctx do
      script_bootstrap_node(ctx.fake, "A1", 1, ReplicasFixture.all_master("test", 1))

      {:ok, pid} = start_tender(ctx, "test")
      :ok = Tender.tend_now(pid)
      assert Tender.ready?(pid)

      tables_before = Tender.tables(pid)
      {:ok, po_before} = PartitionMap.owners(tables_before.owners, "test", 0)

      stop_tender(pid)

      # Re-script the bootstrap cycle — the replacement Tender tends from a
      # clean `state.nodes`, so it will re-do the seed handshake and one
      # partition-generation/peers/replicas round.
      script_bootstrap_node(ctx.fake, "A1", 1, ReplicasFixture.all_master("test", 1))

      {:ok, pid2} = start_tender(ctx, "test")

      # Same table names are exposed by the restarted Tender.
      assert Tender.tables(pid2) == tables_before

      # Rows written by the first Tender survive: owners for partition 0 and
      # the :ready flag are still readable before the new Tender runs any
      # cycle of its own.
      {:ok, po_after_restart} = PartitionMap.owners(tables_before.owners, "test", 0)
      assert po_after_restart == po_before
      assert Tender.ready?(pid2)
    end
  end

  ## Helpers

  defp start_tender(ctx, namespace, opts \\ []) do
    seeds = Keyword.get(opts, :seeds, [{"10.0.0.1", 3000}])

    tender_opts =
      [
        name: ctx.name,
        transport: Fake,
        connect_opts: [fake: ctx.fake],
        seeds: seeds,
        namespaces: [namespace],
        tables: ctx.tables,
        tend_trigger: :manual
      ]
      |> maybe_put(:failure_threshold, Keyword.get(opts, :failure_threshold))

    {:ok, pid} = Tender.start_link(tender_opts)

    on_exit(fn -> stop_tender(pid) end)
    {:ok, pid}
  end

  defp maybe_put(opts, _key, nil), do: opts
  defp maybe_put(opts, key, value), do: Keyword.put(opts, key, value)

  defp script_bootstrap_node(fake, node_name, partition_gen, replicas_value) do
    Fake.script_info(fake, node_name, ["node"], %{"node" => node_name})

    script_cycle(fake, node_name,
      gen: partition_gen,
      peers: "0,3000,[]",
      replicas: replicas_value
    )
  end

  defp script_cycle(fake, node_name, opts) do
    Fake.script_info(fake, node_name, ["partition-generation"], %{
      "partition-generation" => Integer.to_string(Keyword.fetch!(opts, :gen))
    })

    Fake.script_info(fake, node_name, ["peers-clear-std"], %{
      "peers-clear-std" => Keyword.fetch!(opts, :peers)
    })

    Fake.script_info(fake, node_name, ["replicas"], %{
      "replicas" => Keyword.fetch!(opts, :replicas)
    })
  end

  defp register_node(fake, node_name, host, port) do
    :ok = Fake.register_node(fake, node_name, host, port)
  end

  # Valid `replicas` wire body for a node that owns no partitions. The Tender
  # still calls the parser and updates its own failure counter, but no owners
  # entries are written for this node.
  defp empty_replicas do
    ReplicasFixture.build("test", 1, [[]])
  end

  defp stop_tender(pid) do
    if Process.alive?(pid) do
      ref = Process.monitor(pid)
      GenServer.stop(pid, :normal, 2_000)

      receive do
        {:DOWN, ^ref, _, _, _} -> :ok
      after
        2_000 -> :ok
      end
    end
  end

  defp stop_fake(pid), do: stop_process(pid)

  defp stop_process(pid) do
    if Process.alive?(pid) do
      GenServer.stop(pid, :normal, 2_000)
    end
  end
end
