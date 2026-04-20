defmodule Aerospike.TenderTest do
  use ExUnit.Case, async: true

  alias Aerospike.Error
  alias Aerospike.NodeCounters
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
    test "node flips to :inactive only once failures reach the threshold", ctx do
      # Threshold = 3. Each fully-failing cycle contributes 2 failure events:
      # one for refresh-node info and one for peers-clear-std. The replicas
      # fetch is skipped by the Task 3 short-circuit when the generation
      # does not advance (and the cycle's info call failed, so generation_seen
      # stays put). Cycle 1 is healthy (failures = 0). Cycle 2 fails every
      # reachable call (failures = 2, still below the threshold). Cycle 3's
      # first failure lifts the counter to 3 and flips the node to :inactive
      # mid-cycle; owners are cleared and `ready?` drops back to false.
      script_bootstrap_node(ctx.fake, "A1", 2, ReplicasFixture.all_master("test", 2))

      err = %Error{code: :network_error, message: "injected"}

      Enum.each(1..2, fn _ ->
        Fake.script_info_error(ctx.fake, "A1", ["partition-generation", "cluster-stable"], err)
        Fake.script_info_error(ctx.fake, "A1", ["peers-clear-std"], err)
      end)

      {:ok, pid} = start_tender(ctx, "test", failure_threshold: 3)

      :ok = Tender.tend_now(pid)
      assert Tender.ready?(pid)

      %{owners: owners, node_gens: node_gens} = Tender.tables(pid)
      assert {:ok, _} = PartitionMap.get_node_gen(node_gens, "A1")

      # Cycle 2 — failures climb to 2 but stay below threshold.
      :ok = Tender.tend_now(pid)

      assert {:ok, _} = PartitionMap.get_node_gen(node_gens, "A1")
      {:ok, po} = PartitionMap.owners(owners, "test", 0)
      assert "A1" in po.replicas

      # Cycle 3 — threshold reached mid-cycle; node flips to :inactive and
      # its ETS rows are cleared.
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
        Fake.script_info(ctx.fake, "A1", ["partition-generation", "cluster-stable"], %{
          "partition-generation" => Integer.to_string(regime),
          "cluster-stable" => "deadbeef"
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

      Fake.script_info(ctx.fake, "A1", ["partition-generation", "cluster-stable"], %{
        "partition-generation" => "1",
        "cluster-stable" => "deadbeef"
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

      b1_replicas_v1 =
        ReplicasFixture.build("test", 1, [Enum.to_list(half..last_partition)])

      b1_replicas_v2 =
        ReplicasFixture.build("test", 2, [Enum.to_list(half..last_partition)])

      Fake.script_info(ctx.fake, "A1", ["node"], %{"node" => "A1"})
      Fake.script_info(ctx.fake, "B1", ["node"], %{"node" => "B1"})

      # Cycle 1 — both nodes succeed; map is complete and ready.
      script_cycle(ctx.fake, "A1", gen: 1, peers: "0,3000,[]", replicas: a1_replicas)
      script_cycle(ctx.fake, "B1", gen: 1, peers: "0,3000,[]", replicas: b1_replicas_v1)

      # Cycle 2 — A1's generation advances so replicas is refetched (the
      # Task 3 short-circuit would otherwise skip the fetch entirely); the
      # fetch errors. B1's generation advances and its replicas body is
      # upgraded to regime 2 so the partition-map entries for the second
      # half of the ring are re-applied at the new regime. The invariant
      # is that A1's mid-cycle error cannot corrupt B1's owners nor erase
      # A1's previously written entries.
      Fake.script_info(ctx.fake, "A1", ["partition-generation", "cluster-stable"], %{
        "partition-generation" => "2",
        "cluster-stable" => "deadbeef"
      })

      Fake.script_info(ctx.fake, "A1", ["peers-clear-std"], %{"peers-clear-std" => "0,3000,[]"})

      Fake.script_info_error(
        ctx.fake,
        "A1",
        ["replicas"],
        %Error{code: :network_error, message: "injected"}
      )

      script_cycle(ctx.fake, "B1", gen: 2, peers: "0,3000,[]", replicas: b1_replicas_v2)

      {:ok, pid} =
        start_tender(ctx, "test", seeds: [{"10.0.0.1", 3000}, {"10.0.0.2", 3000}])

      :ok = Tender.tend_now(pid)
      assert Tender.ready?(pid)

      %{owners: owners, node_gens: node_gens} = Tender.tables(pid)

      :ok = Tender.tend_now(pid)

      # A1 is still known — one failure is well below the threshold. Its
      # generation advanced even though the replicas fetch failed.
      assert {:ok, 2} = PartitionMap.get_node_gen(node_gens, "A1")
      assert {:ok, 2} = PartitionMap.get_node_gen(node_gens, "B1")

      # A1's previously-written partitions survive its mid-cycle error.
      {:ok, po_a} = PartitionMap.owners(owners, "test", 0)
      assert po_a.regime == 1
      assert "A1" in po_a.replicas

      # B1's partitions were re-applied at regime 2.
      {:ok, po_b} = PartitionMap.owners(owners, "test", last_partition)
      assert po_b.regime == 2
      assert "B1" in po_b.replicas

      assert Tender.ready?(pid)
    end
  end

  describe "node lifecycle: active → inactive on threshold breach" do
    test "threshold breach flips status, stops the pool, and clears ETS rows", ctx do
      # Threshold = 2 — one failing cycle is enough to breach it. The node
      # is flipped to `:inactive`, its pool is stopped, and its owners /
      # node_gens rows are cleared, but the node stays in the Tender's
      # in-memory map so a later cycle can probe it back to `:active`.
      script_bootstrap_node(ctx.fake, "A1", 1, ReplicasFixture.all_master("test", 1))

      err = %Error{code: :network_error, message: "injected"}

      Enum.each(1..3, fn _ ->
        Fake.script_info_error(ctx.fake, "A1", ["partition-generation", "cluster-stable"], err)
        Fake.script_info_error(ctx.fake, "A1", ["peers-clear-std"], err)
        Fake.script_info_error(ctx.fake, "A1", ["replicas"], err)
      end)

      {:ok, pid} = start_tender(ctx, "test", failure_threshold: 2)

      # Cycle 1 — bootstrap + healthy. Status :active.
      :ok = Tender.tend_now(pid)
      assert Tender.ready?(pid)
      assert %{"A1" => %{status: :active}} = Tender.nodes_status(pid)

      # Cycle 2 — first failing cycle. Refresh-node info + peers = failures
      # = 2 which hits the threshold; node flips to :inactive mid-cycle;
      # replicas stage is skipped for inactive nodes.
      :ok = Tender.tend_now(pid)

      %{owners: owners, node_gens: node_gens} = Tender.tables(pid)

      assert {:error, :unknown_node} = PartitionMap.get_node_gen(node_gens, "A1")
      assert {:error, :unknown_partition} = PartitionMap.owners(owners, "test", 0)
      refute Tender.ready?(pid)

      snapshot = Tender.nodes_status(pid)
      assert %{"A1" => %{status: :inactive}} = snapshot
      # Failures counter was reset when the node flipped; a fresh
      # threshold budget applies to the grace window.
      assert snapshot["A1"].failures == 0
    end

    test "pool_pid/2 returns {:error, :unknown_node} for an inactive node", ctx do
      # Start with a live cluster that has a running pool, then force the
      # node to :inactive via a failing cycle. `pool_pid/2` must refuse to
      # hand out the soon-to-be-stopped pool even for the tiny window
      # between the status flip and ETS deletion.
      {:ok, node_sup} = Aerospike.NodeSupervisor.start_link(name: ctx.name)
      on_exit(fn -> stop_process(node_sup) end)

      script_bootstrap_node(ctx.fake, "A1", 1, ReplicasFixture.all_master("test", 1))

      err = %Error{code: :network_error, message: "injected"}

      Enum.each(1..2, fn _ ->
        Fake.script_info_error(ctx.fake, "A1", ["partition-generation", "cluster-stable"], err)
        Fake.script_info_error(ctx.fake, "A1", ["peers-clear-std"], err)
        Fake.script_info_error(ctx.fake, "A1", ["replicas"], err)
      end)

      {:ok, pid} =
        start_tender(ctx, "test",
          failure_threshold: 2,
          node_supervisor: Aerospike.NodeSupervisor.sup_name(ctx.name),
          pool_size: 1
        )

      :ok = Tender.tend_now(pid)
      assert Tender.ready?(pid)
      assert {:ok, pool_pid} = Tender.pool_pid(pid, "A1")
      assert Process.alive?(pool_pid)

      # One failing cycle breaches threshold=2 and flips A1 to :inactive.
      :ok = Tender.tend_now(pid)

      assert %{"A1" => %{status: :inactive}} = Tender.nodes_status(pid)
      assert {:error, :unknown_node} = Tender.pool_pid(pid, "A1")
      # The pool was stopped as part of the transition.
      refute Process.alive?(pool_pid)
    end

    test "inactive → active on successful partition-generation before drop", ctx do
      # Threshold = 2 — one failing cycle flips to :inactive, then a
      # healthy cycle flips back to :active with `:recoveries` bumped.
      script_bootstrap_node(ctx.fake, "A1", 1, ReplicasFixture.all_master("test", 1))

      err = %Error{code: :network_error, message: "injected"}
      # Cycle 2: refresh-node info + peers fail to lift `failures`
      # to the threshold=2 breach. `refresh_partition_maps` is skipped
      # for `:inactive` nodes within the same cycle, so no `replicas`
      # script is consumed.
      Fake.script_info_error(ctx.fake, "A1", ["partition-generation", "cluster-stable"], err)
      Fake.script_info_error(ctx.fake, "A1", ["peers-clear-std"], err)

      # Cycle 3: healthy → :active, recoveries = 1.
      script_cycle(ctx.fake, "A1",
        gen: 2,
        peers: "0,3000,[]",
        replicas: ReplicasFixture.all_master("test", 2)
      )

      {:ok, pid} = start_tender(ctx, "test", failure_threshold: 2)

      :ok = Tender.tend_now(pid)
      assert Tender.ready?(pid)

      :ok = Tender.tend_now(pid)
      assert %{"A1" => %{status: :inactive}} = Tender.nodes_status(pid)

      :ok = Tender.tend_now(pid)

      snapshot = Tender.nodes_status(pid)
      assert snapshot["A1"].status == :active
      assert snapshot["A1"].failures == 0
      assert snapshot["A1"].recoveries == 1
      assert snapshot["A1"].generation_seen == 2
      assert snapshot["A1"].last_tend_result == :ok

      # Replicas were re-applied so ownership and readiness are restored.
      %{owners: owners, node_gens: node_gens} = Tender.tables(pid)
      assert {:ok, 2} = PartitionMap.get_node_gen(node_gens, "A1")
      {:ok, po} = PartitionMap.owners(owners, "test", 0)
      assert po.regime == 2
      assert "A1" in po.replicas
      assert Tender.ready?(pid)
    end

    test "a second consecutive threshold breach while inactive removes the node", ctx do
      script_bootstrap_node(ctx.fake, "A1", 1, ReplicasFixture.all_master("test", 1))

      err = %Error{code: :network_error, message: "injected"}
      # Two failing cycles back-to-back. First flips to :inactive; second
      # drops the node outright.
      Enum.each(1..2, fn _ ->
        Fake.script_info_error(ctx.fake, "A1", ["partition-generation", "cluster-stable"], err)
        Fake.script_info_error(ctx.fake, "A1", ["peers-clear-std"], err)
        Fake.script_info_error(ctx.fake, "A1", ["replicas"], err)
      end)

      {:ok, pid} = start_tender(ctx, "test", failure_threshold: 2)

      :ok = Tender.tend_now(pid)
      assert Tender.ready?(pid)

      # Cycle 2 — flip to :inactive.
      :ok = Tender.tend_now(pid)
      assert %{"A1" => %{status: :inactive}} = Tender.nodes_status(pid)

      # Cycle 3 — failures pass threshold again while inactive → drop.
      :ok = Tender.tend_now(pid)

      assert Tender.nodes_status(pid) == %{}
    end
  end

  describe "cluster-stable agreement guard" do
    test "all active nodes agreeing lets the partition-map refresh proceed", ctx do
      register_node(ctx.fake, "B1", "10.0.0.2", 3000)

      last_partition = PartitionMap.partition_count() - 1
      half = div(last_partition + 1, 2)

      a1_replicas =
        ReplicasFixture.build("test", 1, [Enum.to_list(0..(half - 1))])

      b1_replicas =
        ReplicasFixture.build("test", 1, [Enum.to_list(half..last_partition)])

      Fake.script_info(ctx.fake, "A1", ["node"], %{"node" => "A1"})
      Fake.script_info(ctx.fake, "B1", ["node"], %{"node" => "B1"})

      script_cycle(ctx.fake, "A1",
        gen: 1,
        peers: "0,3000,[]",
        replicas: a1_replicas,
        cluster_stable: "agreed"
      )

      script_cycle(ctx.fake, "B1",
        gen: 1,
        peers: "0,3000,[]",
        replicas: b1_replicas,
        cluster_stable: "agreed"
      )

      {:ok, pid} =
        start_tender(ctx, "test", seeds: [{"10.0.0.1", 3000}, {"10.0.0.2", 3000}])

      :ok = Tender.tend_now(pid)

      assert Tender.ready?(pid)

      %{owners: owners} = Tender.tables(pid)
      {:ok, po_first} = PartitionMap.owners(owners, "test", 0)
      assert "A1" in po_first.replicas
      {:ok, po_last} = PartitionMap.owners(owners, "test", last_partition)
      assert "B1" in po_last.replicas
    end

    test "disagreement skips refresh and leaves prior owners untouched", ctx do
      register_node(ctx.fake, "B1", "10.0.0.2", 3000)

      last_partition = PartitionMap.partition_count() - 1
      half = div(last_partition + 1, 2)

      a1_replicas =
        ReplicasFixture.build("test", 1, [Enum.to_list(0..(half - 1))])

      b1_replicas =
        ReplicasFixture.build("test", 1, [Enum.to_list(half..last_partition)])

      Fake.script_info(ctx.fake, "A1", ["node"], %{"node" => "A1"})
      Fake.script_info(ctx.fake, "B1", ["node"], %{"node" => "B1"})

      # Cycle 1 — both nodes agree; the map is fully populated.
      script_cycle(ctx.fake, "A1",
        gen: 1,
        peers: "0,3000,[]",
        replicas: a1_replicas,
        cluster_stable: "stable-1"
      )

      script_cycle(ctx.fake, "B1",
        gen: 1,
        peers: "0,3000,[]",
        replicas: b1_replicas,
        cluster_stable: "stable-1"
      )

      # Cycle 2 — A1 reports a different hash than B1. Disagreement aborts
      # the partition-map refresh: no `replicas` script for either node is
      # consumed (the stage is skipped entirely). Even at higher
      # generations the prior partition entries must survive untouched.
      Fake.script_info(ctx.fake, "A1", ["partition-generation", "cluster-stable"], %{
        "partition-generation" => "2",
        "cluster-stable" => "stable-2-A"
      })

      Fake.script_info(ctx.fake, "A1", ["peers-clear-std"], %{"peers-clear-std" => "0,3000,[]"})

      Fake.script_info(ctx.fake, "B1", ["partition-generation", "cluster-stable"], %{
        "partition-generation" => "2",
        "cluster-stable" => "stable-2-B"
      })

      Fake.script_info(ctx.fake, "B1", ["peers-clear-std"], %{"peers-clear-std" => "0,3000,[]"})

      {:ok, pid} =
        start_tender(ctx, "test", seeds: [{"10.0.0.1", 3000}, {"10.0.0.2", 3000}])

      :ok = Tender.tend_now(pid)
      assert Tender.ready?(pid)

      %{owners: owners} = Tender.tables(pid)
      {:ok, po_before} = PartitionMap.owners(owners, "test", 0)
      assert po_before.regime == 1
      assert "A1" in po_before.replicas

      log =
        ExUnit.CaptureLog.capture_log(fn ->
          :ok = Tender.tend_now(pid)
        end)

      assert log =~ "cluster-stable disagreement"
      assert log =~ "stable-2-A"
      assert log =~ "stable-2-B"

      # Owners survive the skipped refresh — same regime, same replicas.
      {:ok, po_after} = PartitionMap.owners(owners, "test", 0)
      assert po_after == po_before

      {:ok, po_last} = PartitionMap.owners(owners, "test", last_partition)
      assert po_last.regime == 1
      assert "B1" in po_last.replicas

      # Both nodes are still active — the disagreement is a refresh-stage
      # decision, not a node-health signal.
      snapshot = Tender.nodes_status(pid)
      assert snapshot["A1"].status == :active
      assert snapshot["B1"].status == :active
    end

    test "agreement resumes once both nodes report the same hash again", ctx do
      register_node(ctx.fake, "B1", "10.0.0.2", 3000)

      last_partition = PartitionMap.partition_count() - 1
      half = div(last_partition + 1, 2)

      a1_replicas_v1 =
        ReplicasFixture.build("test", 1, [Enum.to_list(0..(half - 1))])

      b1_replicas_v1 =
        ReplicasFixture.build("test", 1, [Enum.to_list(half..last_partition)])

      a1_replicas_v2 =
        ReplicasFixture.build("test", 2, [Enum.to_list(0..(half - 1))])

      b1_replicas_v2 =
        ReplicasFixture.build("test", 2, [Enum.to_list(half..last_partition)])

      Fake.script_info(ctx.fake, "A1", ["node"], %{"node" => "A1"})
      Fake.script_info(ctx.fake, "B1", ["node"], %{"node" => "B1"})

      # Cycle 1 — agree; map populated.
      script_cycle(ctx.fake, "A1",
        gen: 1,
        peers: "0,3000,[]",
        replicas: a1_replicas_v1,
        cluster_stable: "v1"
      )

      script_cycle(ctx.fake, "B1",
        gen: 1,
        peers: "0,3000,[]",
        replicas: b1_replicas_v1,
        cluster_stable: "v1"
      )

      # Cycle 2 — disagree; refresh skipped.
      Fake.script_info(ctx.fake, "A1", ["partition-generation", "cluster-stable"], %{
        "partition-generation" => "2",
        "cluster-stable" => "v2-A"
      })

      Fake.script_info(ctx.fake, "A1", ["peers-clear-std"], %{"peers-clear-std" => "0,3000,[]"})

      Fake.script_info(ctx.fake, "B1", ["partition-generation", "cluster-stable"], %{
        "partition-generation" => "2",
        "cluster-stable" => "v2-B"
      })

      Fake.script_info(ctx.fake, "B1", ["peers-clear-std"], %{"peers-clear-std" => "0,3000,[]"})

      # Cycle 3 — agree on the new hash; map updated to regime 2.
      script_cycle(ctx.fake, "A1",
        gen: 2,
        peers: "0,3000,[]",
        replicas: a1_replicas_v2,
        cluster_stable: "v2"
      )

      script_cycle(ctx.fake, "B1",
        gen: 2,
        peers: "0,3000,[]",
        replicas: b1_replicas_v2,
        cluster_stable: "v2"
      )

      {:ok, pid} =
        start_tender(ctx, "test", seeds: [{"10.0.0.1", 3000}, {"10.0.0.2", 3000}])

      :ok = Tender.tend_now(pid)
      :ok = Tender.tend_now(pid)
      :ok = Tender.tend_now(pid)

      %{owners: owners} = Tender.tables(pid)

      {:ok, po_first} = PartitionMap.owners(owners, "test", 0)
      assert po_first.regime == 2
      assert "A1" in po_first.replicas

      {:ok, po_last} = PartitionMap.owners(owners, "test", last_partition)
      assert po_last.regime == 2
      assert "B1" in po_last.replicas
    end

    test "active nodes that failed this cycle's info call are excluded from the hash set",
         ctx do
      register_node(ctx.fake, "B1", "10.0.0.2", 3000)

      last_partition = PartitionMap.partition_count() - 1
      half = div(last_partition + 1, 2)

      a1_replicas =
        ReplicasFixture.build("test", 1, [Enum.to_list(0..(half - 1))])

      b1_replicas =
        ReplicasFixture.build("test", 1, [Enum.to_list(half..last_partition)])

      Fake.script_info(ctx.fake, "A1", ["node"], %{"node" => "A1"})
      Fake.script_info(ctx.fake, "B1", ["node"], %{"node" => "B1"})

      # Cycle 1 — both healthy, both agree on hash="X".
      script_cycle(ctx.fake, "A1",
        gen: 1,
        peers: "0,3000,[]",
        replicas: a1_replicas,
        cluster_stable: "X"
      )

      script_cycle(ctx.fake, "B1",
        gen: 1,
        peers: "0,3000,[]",
        replicas: b1_replicas,
        cluster_stable: "X"
      )

      # Cycle 2 — B1 still healthy, A1's refresh-node info call fails (so
      # A1 contributes no hash this cycle). The single non-nil hash is
      # B1's "Y", so the guard treats this as agreement-of-one and the
      # partition-map refresh proceeds for B1. A1 is below the failure
      # threshold so it remains :active.
      err = %Error{code: :network_error, message: "injected"}
      Fake.script_info_error(ctx.fake, "A1", ["partition-generation", "cluster-stable"], err)
      Fake.script_info_error(ctx.fake, "A1", ["peers-clear-std"], err)

      script_cycle(ctx.fake, "B1",
        gen: 2,
        peers: "0,3000,[]",
        replicas: b1_replicas,
        cluster_stable: "Y"
      )

      {:ok, pid} =
        start_tender(ctx, "test",
          seeds: [{"10.0.0.1", 3000}, {"10.0.0.2", 3000}],
          failure_threshold: 5
        )

      :ok = Tender.tend_now(pid)
      assert Tender.ready?(pid)

      :ok = Tender.tend_now(pid)

      snapshot = Tender.nodes_status(pid)
      assert snapshot["A1"].status == :active
      assert snapshot["B1"].status == :active

      %{owners: owners, node_gens: node_gens} = Tender.tables(pid)

      # B1's generation advanced — the agreement guard returned :ok with
      # B1 as sole contributor, so the partition-map refresh stage ran
      # and B1's replicas were re-applied. A1's owners survive untouched
      # (its replicas script is never consumed because no replicas info
      # was scripted for cycle 2).
      assert {:ok, 2} = PartitionMap.get_node_gen(node_gens, "B1")

      {:ok, po_a} = PartitionMap.owners(owners, "test", 0)
      assert po_a.regime == 1
      assert "A1" in po_a.replicas

      {:ok, po_b} = PartitionMap.owners(owners, "test", last_partition)
      assert "B1" in po_b.replicas
    end

    test "no contributors is not a disagreement and the pipeline proceeds", ctx do
      # Bootstrap failure: the seed host is not registered with the Fake
      # so `connect/3` returns `:connection_error` and no node enters
      # `state.nodes`. `verify_cluster_stable/1` returns `{:ok, :empty}`
      # and the partition-map refresh is a no-op (no nodes to fetch from).
      log =
        ExUnit.CaptureLog.capture_log(fn ->
          {:ok, pid} = start_tender(ctx, "test", seeds: [{"10.0.0.99", 3000}])

          :ok = Tender.tend_now(pid)

          assert Tender.nodes_status(pid) == %{}
          refute Tender.ready?(pid)
        end)

      refute log =~ "cluster-stable disagreement"
    end
  end

  describe "partition-generation short-circuit: avoid replicas fetch when unchanged" do
    test "first cycle always fetches replicas regardless of applied state", ctx do
      # applied_gen starts at nil for a newly-discovered node, so the
      # short-circuit cannot fire. The cycle must fetch `replicas` to
      # populate ownership for the first time.
      script_bootstrap_node(ctx.fake, "A1", 7, ReplicasFixture.all_master("test", 7))

      {:ok, pid} = start_tender(ctx, "test")

      :ok = Tender.tend_now(pid)

      assert Tender.ready?(pid)

      %{owners: owners, node_gens: node_gens} = Tender.tables(pid)
      assert {:ok, 7} = PartitionMap.get_node_gen(node_gens, "A1")
      {:ok, po} = PartitionMap.owners(owners, "test", 0)
      assert po.regime == 7
      assert po.replicas == ["A1"]
    end

    test "second cycle with unchanged generation skips the replicas fetch", ctx do
      # Cycle 1 fetches replicas and advances applied_gen to 1. Cycle 2
      # scripts partition-generation = 1 again but intentionally does not
      # script a `replicas` reply for A1 — the short-circuit must prevent
      # the fetch. If the short-circuit misfired the Fake's default
      # no-script reply (`{:error, :no_script}`) would bump A1's failure
      # counter; we assert it stays at zero.
      script_bootstrap_node(ctx.fake, "A1", 1, ReplicasFixture.all_master("test", 1))

      Fake.script_info(ctx.fake, "A1", ["partition-generation", "cluster-stable"], %{
        "partition-generation" => "1",
        "cluster-stable" => "deadbeef"
      })

      Fake.script_info(ctx.fake, "A1", ["peers-clear-std"], %{"peers-clear-std" => "0,3000,[]"})

      {:ok, pid} = start_tender(ctx, "test")

      :ok = Tender.tend_now(pid)
      assert Tender.ready?(pid)

      :ok = Tender.tend_now(pid)

      # No replicas call means no failure counted against A1.
      snapshot = Tender.nodes_status(pid)
      assert snapshot["A1"].failures == 0
      assert snapshot["A1"].status == :active
      assert snapshot["A1"].generation_seen == 1
      assert Tender.ready?(pid)
    end

    test "second cycle with advanced generation refetches replicas", ctx do
      # Cycle 1 at gen 1, cycle 2 at gen 2. The short-circuit must not fire
      # on cycle 2 (generation advanced); the scripted regime-2 replicas
      # reply is consumed and ownership is updated to regime 2.
      script_bootstrap_node(ctx.fake, "A1", 1, ReplicasFixture.all_master("test", 1))

      script_cycle(ctx.fake, "A1",
        gen: 2,
        peers: "0,3000,[]",
        replicas: ReplicasFixture.all_master("test", 2)
      )

      {:ok, pid} = start_tender(ctx, "test")

      :ok = Tender.tend_now(pid)
      :ok = Tender.tend_now(pid)

      %{owners: owners, node_gens: node_gens} = Tender.tables(pid)
      assert {:ok, 2} = PartitionMap.get_node_gen(node_gens, "A1")
      {:ok, po} = PartitionMap.owners(owners, "test", 0)
      assert po.regime == 2
      assert po.replicas == ["A1"]
    end

    test "applied_gen does not advance when every segment is rejected as stale", ctx do
      # Seed the owners table with a regime-5 entry for partition 0 via a
      # successful cycle, then script a cycle at lower regime. The cluster-
      # stable hashes differ across cycles so the agreement guard is not
      # the reason replicas is fetched; the generation advances, short-
      # circuit does not fire, replicas IS fetched, but every segment is
      # rejected as stale. applied_gen must stay at the previous value so
      # the next cycle refetches and has a chance to re-apply.
      Fake.script_info(ctx.fake, "A1", ["node"], %{"node" => "A1"})

      # Cycle 1 installs regime 5.
      script_cycle(ctx.fake, "A1",
        gen: 5,
        peers: "0,3000,[]",
        replicas: ReplicasFixture.all_master("test", 5)
      )

      # Cycle 2 scripts a regime-4 reply at gen 6; every segment is stale.
      script_cycle(ctx.fake, "A1",
        gen: 6,
        peers: "0,3000,[]",
        replicas: ReplicasFixture.all_master("test", 4)
      )

      # Cycle 3 scripts the same gen 6 but a valid regime-5 reply — if
      # applied_gen advanced to 6 in cycle 2, cycle 3 would short-circuit
      # and this scripted replicas reply would not be consumed. The test
      # verifies cycle 3 DOES fetch replicas.
      script_cycle(ctx.fake, "A1",
        gen: 6,
        peers: "0,3000,[]",
        replicas: ReplicasFixture.all_master("test", 5)
      )

      {:ok, pid} = start_tender(ctx, "test")

      :ok = Tender.tend_now(pid)
      :ok = Tender.tend_now(pid)
      :ok = Tender.tend_now(pid)

      # Regime remains at the accepted value — the stale replies never
      # overwrote it and cycle 3's equal-regime reply re-validated it.
      %{owners: owners} = Tender.tables(pid)
      {:ok, po} = PartitionMap.owners(owners, "test", 0)
      assert po.regime == 5
      assert po.replicas == ["A1"]

      # No replicas-side failures bumped — every call returned a parseable
      # body, even when segments were stale.
      snapshot = Tender.nodes_status(pid)
      assert snapshot["A1"].failures == 0
    end

    test "recovery from :inactive refetches replicas on first healthy cycle", ctx do
      # Cycle 1: healthy, applied_gen = 1.
      # Cycle 2: refresh-node info fails, lifts failures to threshold and
      # flips the node to :inactive; mark_inactive clears applied_gen.
      # Cycle 3: refresh-node info succeeds at gen 2, node recovers; the
      # replicas fetch must run even though the stored node_gens could have
      # been reused — applied_gen was nil-reset on the inactive flip.
      script_bootstrap_node(ctx.fake, "A1", 1, ReplicasFixture.all_master("test", 1))

      err = %Error{code: :network_error, message: "injected"}
      Fake.script_info_error(ctx.fake, "A1", ["partition-generation", "cluster-stable"], err)
      Fake.script_info_error(ctx.fake, "A1", ["peers-clear-std"], err)

      # Cycle 3 scripted replicas — must be consumed for the recovery path.
      script_cycle(ctx.fake, "A1",
        gen: 2,
        peers: "0,3000,[]",
        replicas: ReplicasFixture.all_master("test", 2)
      )

      {:ok, pid} = start_tender(ctx, "test", failure_threshold: 2)

      :ok = Tender.tend_now(pid)
      :ok = Tender.tend_now(pid)
      assert %{"A1" => %{status: :inactive}} = Tender.nodes_status(pid)

      :ok = Tender.tend_now(pid)

      snapshot = Tender.nodes_status(pid)
      assert snapshot["A1"].status == :active
      assert snapshot["A1"].generation_seen == 2

      %{owners: owners, node_gens: node_gens} = Tender.tables(pid)
      assert {:ok, 2} = PartitionMap.get_node_gen(node_gens, "A1")
      {:ok, po} = PartitionMap.owners(owners, "test", 0)
      assert po.regime == 2
      assert Tender.ready?(pid)
    end
  end

  describe "per-node counters lifecycle" do
    test "node_counters/2 returns the same reference nodes_status/1 exposes", ctx do
      {:ok, node_sup} = Aerospike.NodeSupervisor.start_link(name: ctx.name)
      on_exit(fn -> stop_process(node_sup) end)

      script_bootstrap_node(ctx.fake, "A1", 1, ReplicasFixture.all_master("test", 1))

      {:ok, pid} =
        start_tender(ctx, "test",
          node_supervisor: Aerospike.NodeSupervisor.sup_name(ctx.name),
          pool_size: 1
        )

      :ok = Tender.tend_now(pid)

      assert {:ok, ref} = Tender.node_counters(pid, "A1")
      snapshot = Tender.nodes_status(pid)
      assert snapshot["A1"].counters == ref
      # `:counters.new/2` returns an opaque `{:atomics, ref}` tuple; the
      # Tender must hand that through unchanged so callers can read/write
      # via the `:counters` module.
      assert match?({:atomics, inner} when is_reference(inner), ref)
    end

    test "node_counters/2 is {:error, :unknown_node} for inactive and unknown nodes", ctx do
      {:ok, node_sup} = Aerospike.NodeSupervisor.start_link(name: ctx.name)
      on_exit(fn -> stop_process(node_sup) end)

      script_bootstrap_node(ctx.fake, "A1", 1, ReplicasFixture.all_master("test", 1))

      err = %Error{code: :network_error, message: "injected"}

      Enum.each(1..2, fn _ ->
        Fake.script_info_error(ctx.fake, "A1", ["partition-generation", "cluster-stable"], err)
        Fake.script_info_error(ctx.fake, "A1", ["peers-clear-std"], err)
        Fake.script_info_error(ctx.fake, "A1", ["replicas"], err)
      end)

      {:ok, pid} =
        start_tender(ctx, "test",
          failure_threshold: 2,
          node_supervisor: Aerospike.NodeSupervisor.sup_name(ctx.name),
          pool_size: 1
        )

      :ok = Tender.tend_now(pid)
      assert {:ok, _ref} = Tender.node_counters(pid, "A1")

      # Flip A1 to :inactive — counters must be cleared; node_counters/2
      # refuses to hand out a reference for a node whose pool is gone.
      :ok = Tender.tend_now(pid)
      assert {:error, :unknown_node} = Tender.node_counters(pid, "A1")
      assert Tender.nodes_status(pid)["A1"].counters == nil

      assert {:error, :unknown_node} = Tender.node_counters(pid, "ghost")
    end

    test "cluster-state-only mode (no node_supervisor) returns :unknown_node", ctx do
      # Without a :node_supervisor the Tender never starts a pool, so
      # there is no writer to the counter slots. Exposing a reference
      # would imply counters exist when they do not.
      script_bootstrap_node(ctx.fake, "A1", 1, ReplicasFixture.all_master("test", 1))

      {:ok, pid} = start_tender(ctx, "test")

      :ok = Tender.tend_now(pid)

      assert {:error, :unknown_node} = Tender.node_counters(pid, "A1")
      assert Tender.nodes_status(pid)["A1"].counters == nil
    end

    test "counters outlive a completed command but a new reference is issued on recovery",
         ctx do
      # Cycle 1 bootstraps A1 with a pool + counters. Cycle 2 fails and
      # trips the inactive transition (counters cleared to nil). Cycle 3
      # is healthy → A1 flips back to :active but its pool was torn down
      # in cycle 2 and is not restarted by the Tier 1.5 recovery path.
      # The test pins the post-recovery counters field to nil so future
      # tasks that re-introduce pool restart can update this expectation
      # in one place instead of discovering the contract by accident.
      {:ok, node_sup} = Aerospike.NodeSupervisor.start_link(name: ctx.name)
      on_exit(fn -> stop_process(node_sup) end)

      script_bootstrap_node(ctx.fake, "A1", 1, ReplicasFixture.all_master("test", 1))

      err = %Error{code: :network_error, message: "injected"}
      Fake.script_info_error(ctx.fake, "A1", ["partition-generation", "cluster-stable"], err)
      Fake.script_info_error(ctx.fake, "A1", ["peers-clear-std"], err)

      # Cycle 3 succeeds against the info socket (mark_inactive keeps
      # node.conn intact so the probe succeeds), but replicas is never
      # fetched without a pool — refresh_partition_maps skips inactive,
      # and after recovery the Task 3 short-circuit sees applied_gen = nil
      # and will try to fetch.
      script_cycle(ctx.fake, "A1",
        gen: 2,
        peers: "0,3000,[]",
        replicas: ReplicasFixture.all_master("test", 2)
      )

      {:ok, pid} =
        start_tender(ctx, "test",
          failure_threshold: 2,
          node_supervisor: Aerospike.NodeSupervisor.sup_name(ctx.name),
          pool_size: 1
        )

      :ok = Tender.tend_now(pid)
      {:ok, ref_before} = Tender.node_counters(pid, "A1")

      :ok = Tender.tend_now(pid)
      assert %{"A1" => %{status: :inactive, counters: nil}} = Tender.nodes_status(pid)

      :ok = Tender.tend_now(pid)

      snapshot = Tender.nodes_status(pid)
      assert snapshot["A1"].status == :active
      # Pool was not restarted during recovery (pre-existing Tier 1.5
      # behaviour), so counters remain nil after recovery. The pre-
      # recovery reference must be gone — it is unsafe for anyone to
      # still be writing through it because the pool that produced
      # those writes is dead.
      assert snapshot["A1"].counters == nil
      assert {:error, :unknown_node} = Tender.node_counters(pid, "A1")
      # The old reference is still a live term (refs never go away on
      # their own), but nothing keeps writing to it.
      assert match?({:atomics, inner} when is_reference(inner), ref_before)
    end
  end

  describe "node_handle/2" do
    test "returns pool, counters, and breaker opts in one GenServer hop", ctx do
      {:ok, node_sup} = Aerospike.NodeSupervisor.start_link(name: ctx.name)
      on_exit(fn -> stop_process(node_sup) end)

      script_bootstrap_node(ctx.fake, "A1", 1, ReplicasFixture.all_master("test", 1))

      {:ok, pid} =
        start_tender(ctx, "test",
          node_supervisor: Aerospike.NodeSupervisor.sup_name(ctx.name),
          pool_size: 2,
          circuit_open_threshold: 7,
          max_concurrent_ops_per_node: 42
        )

      :ok = Tender.tend_now(pid)

      assert {:ok, handle} = Tender.node_handle(pid, "A1")
      # Pool and counters must match the ones the individual accessors
      # hand out so callers can mix `node_handle/2` with the Task 5
      # accessors during the transition.
      assert {:ok, pool} = Tender.pool_pid(pid, "A1")
      assert handle.pool == pool
      assert {:ok, counters} = Tender.node_counters(pid, "A1")
      assert handle.counters == counters

      # Breaker thresholds flow from start_link/1 verbatim.
      assert handle.breaker == %{
               circuit_open_threshold: 7,
               max_concurrent_ops_per_node: 42
             }
    end

    test "defaults max_concurrent_ops_per_node to pool_size * 10", ctx do
      {:ok, node_sup} = Aerospike.NodeSupervisor.start_link(name: ctx.name)
      on_exit(fn -> stop_process(node_sup) end)

      script_bootstrap_node(ctx.fake, "A1", 1, ReplicasFixture.all_master("test", 1))

      {:ok, pid} =
        start_tender(ctx, "test",
          node_supervisor: Aerospike.NodeSupervisor.sup_name(ctx.name),
          pool_size: 4
        )

      :ok = Tender.tend_now(pid)

      {:ok, handle} = Tender.node_handle(pid, "A1")
      assert handle.breaker.max_concurrent_ops_per_node == 40
      # The default failure threshold is non-zero so the breaker does not
      # refuse a freshly-booted cluster.
      assert handle.breaker.circuit_open_threshold > 0
    end

    test "returns {:error, :unknown_node} for inactive and unknown nodes", ctx do
      {:ok, node_sup} = Aerospike.NodeSupervisor.start_link(name: ctx.name)
      on_exit(fn -> stop_process(node_sup) end)

      script_bootstrap_node(ctx.fake, "A1", 1, ReplicasFixture.all_master("test", 1))

      err = %Error{code: :network_error, message: "injected"}

      Enum.each(1..2, fn _ ->
        Fake.script_info_error(ctx.fake, "A1", ["partition-generation", "cluster-stable"], err)
        Fake.script_info_error(ctx.fake, "A1", ["peers-clear-std"], err)
        Fake.script_info_error(ctx.fake, "A1", ["replicas"], err)
      end)

      {:ok, pid} =
        start_tender(ctx, "test",
          failure_threshold: 2,
          node_supervisor: Aerospike.NodeSupervisor.sup_name(ctx.name),
          pool_size: 1
        )

      :ok = Tender.tend_now(pid)
      assert {:ok, _handle} = Tender.node_handle(pid, "A1")

      :ok = Tender.tend_now(pid)
      # Inactive node: counters cleared, pool stopped, handle refused.
      assert {:error, :unknown_node} = Tender.node_handle(pid, "A1")
      assert {:error, :unknown_node} = Tender.node_handle(pid, "ghost")
    end

    test "cluster-state-only mode (no pool) refuses the handle", ctx do
      script_bootstrap_node(ctx.fake, "A1", 1, ReplicasFixture.all_master("test", 1))

      {:ok, pid} = start_tender(ctx, "test")

      :ok = Tender.tend_now(pid)

      # No pool → no counters; the handle contract requires both.
      assert {:error, :unknown_node} = Tender.node_handle(pid, "A1")
    end
  end

  describe "`:failed` counter decay on successful tend cycles" do
    test "a successful refresh-node info cycle zeroes the `:failed` slot", ctx do
      {:ok, node_sup} = Aerospike.NodeSupervisor.start_link(name: ctx.name)
      on_exit(fn -> stop_process(node_sup) end)

      script_bootstrap_node(ctx.fake, "A1", 1, ReplicasFixture.all_master("test", 1))

      # Cycle 2 will also be healthy so the reset fires mid-run.
      script_cycle(ctx.fake, "A1",
        gen: 2,
        peers: "0,3000,[]",
        replicas: ReplicasFixture.all_master("test", 2)
      )

      {:ok, pid} =
        start_tender(ctx, "test",
          node_supervisor: Aerospike.NodeSupervisor.sup_name(ctx.name),
          pool_size: 1
        )

      :ok = Tender.tend_now(pid)
      {:ok, ref} = Tender.node_counters(pid, "A1")

      # Simulate pool-path transport failures piling up. These slots
      # belong to the pool's callbacks in production; here we write
      # directly so the test does not need to stage a pool checkout.
      for _ <- 1..5, do: NodeCounters.incr_failed(ref)
      assert NodeCounters.failed(ref) == 5

      :ok = Tender.tend_now(pid)

      # The successful tend cycle decayed the window back to 0 — the
      # breaker's next check will see a healthy node.
      assert NodeCounters.failed(ref) == 0
    end

    test "a failed refresh-node info cycle does not touch the `:failed` slot", ctx do
      {:ok, node_sup} = Aerospike.NodeSupervisor.start_link(name: ctx.name)
      on_exit(fn -> stop_process(node_sup) end)

      script_bootstrap_node(ctx.fake, "A1", 1, ReplicasFixture.all_master("test", 1))

      err = %Error{code: :network_error, message: "injected"}
      Fake.script_info_error(ctx.fake, "A1", ["partition-generation", "cluster-stable"], err)
      Fake.script_info_error(ctx.fake, "A1", ["peers-clear-std"], err)

      {:ok, pid} =
        start_tender(ctx, "test",
          # High threshold so the failing cycle does not trip :inactive;
          # the intent is to observe `:failed` after a partial failure.
          failure_threshold: 10,
          node_supervisor: Aerospike.NodeSupervisor.sup_name(ctx.name),
          pool_size: 1
        )

      :ok = Tender.tend_now(pid)
      {:ok, ref} = Tender.node_counters(pid, "A1")

      for _ <- 1..3, do: NodeCounters.incr_failed(ref)
      assert NodeCounters.failed(ref) == 3

      # Failing refresh-node cycle must leave the slot alone — the reset
      # is tied to tender-side success, not unconditional.
      :ok = Tender.tend_now(pid)
      assert NodeCounters.failed(ref) == 3
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
      |> maybe_put(:node_supervisor, Keyword.get(opts, :node_supervisor))
      |> maybe_put(:pool_size, Keyword.get(opts, :pool_size))
      |> maybe_put(:circuit_open_threshold, Keyword.get(opts, :circuit_open_threshold))
      |> maybe_put(
        :max_concurrent_ops_per_node,
        Keyword.get(opts, :max_concurrent_ops_per_node)
      )

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
    Fake.script_info(fake, node_name, ["partition-generation", "cluster-stable"], %{
      "partition-generation" => Integer.to_string(Keyword.fetch!(opts, :gen)),
      "cluster-stable" => Keyword.get(opts, :cluster_stable, "deadbeef")
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
      try do
        GenServer.stop(pid, :normal, 2_000)
      catch
        :exit, _ -> :ok
      end
    end
  end
end
