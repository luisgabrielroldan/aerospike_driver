defmodule Aerospike.TenderTest do
  use ExUnit.Case, async: true

  alias Aerospike.Error
  alias Aerospike.NodeCounters
  alias Aerospike.PartitionMap
  alias Aerospike.TableOwner
  alias Aerospike.Tender
  alias Aerospike.Test.ReplicasFixture
  alias Aerospike.Transport.Fake

  def forward(event, measurements, metadata, test_pid) do
    send(test_pid, {:event, event, measurements, metadata})
  end

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

  describe "bootstrap captures the node's `features` info-key reply" do
    test "recognised features land on the node record and the snapshot", ctx do
      Fake.script_info(ctx.fake, "A1", ["node", "features"], %{
        "node" => "A1",
        "features" => "compression;pipelining"
      })

      script_cycle(ctx.fake, "A1",
        gen: 1,
        peers: "0,3000,[]",
        replicas: ReplicasFixture.all_master("test", 1)
      )

      {:ok, pid} = start_tender(ctx, "test")
      :ok = Tender.tend_now(pid)

      snapshot = Tender.nodes_status(pid)
      assert MapSet.new([:compression, :pipelining]) == snapshot["A1"].features
    end

    test "unknown tokens are preserved as {:unknown, raw} so future probes can see them", ctx do
      Fake.script_info(ctx.fake, "A1", ["node", "features"], %{
        "node" => "A1",
        "features" => "compression;peers;batch-index"
      })

      script_cycle(ctx.fake, "A1",
        gen: 1,
        peers: "0,3000,[]",
        replicas: ReplicasFixture.all_master("test", 1)
      )

      {:ok, pid} = start_tender(ctx, "test")
      :ok = Tender.tend_now(pid)

      features = Tender.nodes_status(pid)["A1"].features
      assert MapSet.member?(features, :compression)
      assert MapSet.member?(features, {:unknown, "peers"})
      assert MapSet.member?(features, {:unknown, "batch-index"})
    end

    test "missing `features` key registers the node with an empty MapSet", ctx do
      Fake.script_info(ctx.fake, "A1", ["node", "features"], %{"node" => "A1"})

      script_cycle(ctx.fake, "A1",
        gen: 1,
        peers: "0,3000,[]",
        replicas: ReplicasFixture.all_master("test", 1)
      )

      {:ok, pid} = start_tender(ctx, "test")
      :ok = Tender.tend_now(pid)

      assert MapSet.new() == Tender.nodes_status(pid)["A1"].features
    end

    test "empty `features` value still registers the node with an empty MapSet", ctx do
      script_bootstrap_node(ctx.fake, "A1", 1, ReplicasFixture.all_master("test", 1))

      {:ok, pid} = start_tender(ctx, "test")
      :ok = Tender.tend_now(pid)

      assert MapSet.new() == Tender.nodes_status(pid)["A1"].features
    end

    test "peer-discovered nodes capture features from a dedicated probe", ctx do
      register_node(ctx.fake, "B1", "10.0.0.2", 3000)

      script_bootstrap_info(ctx.fake, "A1")
      script_bootstrap_info(ctx.fake, "B1", "compression")

      script_cycle(ctx.fake, "A1",
        gen: 1,
        peers: "1,3000,[[B1,,[10.0.0.2:3000]]]",
        replicas: ReplicasFixture.all_master("test", 1)
      )

      # B1 is reached via ensure_peer_connected, which probes only the
      # `features` key on the freshly opened socket.
      Fake.script_info(ctx.fake, "B1", ["features"], %{
        "features" => "compression;pipelining"
      })

      Fake.script_info(ctx.fake, "B1", ["replicas"], %{
        "replicas" => ReplicasFixture.all_master("test", 1)
      })

      {:ok, pid} = start_tender(ctx, "test")
      :ok = Tender.tend_now(pid)

      snapshot = Tender.nodes_status(pid)
      assert MapSet.new([:compression, :pipelining]) == snapshot["B1"].features
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
      script_bootstrap_info(ctx.fake, "A1")

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

      script_bootstrap_info(ctx.fake, "A1")

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
      script_bootstrap_info(ctx.fake, "A1")
      script_bootstrap_info(ctx.fake, "B1")

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
      # ensure_peer_connected probes the peer's features after dialling so the
      # node record carries the same MapSet shape as a seed-registered node.
      Fake.script_info(ctx.fake, "C1", ["features"], %{"features" => ""})
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

      script_bootstrap_info(ctx.fake, "A1")
      script_bootstrap_info(ctx.fake, "B1")

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

  describe "re-bootstrap when state.nodes goes empty" do
    test "steady state never re-dials seeds after the first bootstrap", ctx do
      # Script five healthy tend cycles against a single node. Once the
      # Tender has a non-empty `state.nodes`, `bootstrap_if_needed/1` must
      # short-circuit and the Fake transport must record exactly one
      # `connect/3` call against the seed for the whole run.
      script_bootstrap_node(ctx.fake, "A1", 1, ReplicasFixture.all_master("test", 1))

      Enum.each(1..4, fn _ ->
        Fake.script_info(ctx.fake, "A1", ["partition-generation", "cluster-stable"], %{
          "partition-generation" => "1",
          "cluster-stable" => "deadbeef"
        })

        Fake.script_info(ctx.fake, "A1", ["peers-clear-std"], %{
          "peers-clear-std" => "0,3000,[]"
        })
      end)

      {:ok, pid} = start_tender(ctx, "test")

      Enum.each(1..5, fn _ -> :ok = Tender.tend_now(pid) end)

      assert Fake.connect_count(ctx.fake, "A1") == 1,
             "seed must be dialled exactly once in steady state"
    end

    test "re-dials seeds after every node has been dropped", ctx do
      # Cycle 1 bootstraps A1 successfully. Cycle 2 fails the refresh-node
      # info call and the peers probe, driving A1's failures to the
      # threshold and flipping it to :inactive (the replicas fetch is
      # skipped because the :inactive filter in `refresh_partition_maps/1`
      # sees A1 after the flip). Cycle 3 probes A1's pg/cluster-stable
      # through `refresh_nodes/1` and fails again — because A1 is already
      # :inactive, that single failure removes the node entirely. At that
      # point `state.nodes == %{}` and the next tend cycle must re-run
      # `bootstrap_seed/3` against the configured seeds. `connect_count`
      # must advance to 2 and A1 must come back as :active with a fresh
      # partition map applied.
      script_bootstrap_node(ctx.fake, "A1", 1, ReplicasFixture.all_master("test", 1))

      err = %Error{code: :network_error, message: "injected"}

      # Cycle 2 — fails pg/cluster-stable and peers; mark_inactive flips
      # A1 to :inactive before the replicas stage runs, so no replicas
      # script is consumed.
      Fake.script_info_error(ctx.fake, "A1", ["partition-generation", "cluster-stable"], err)
      Fake.script_info_error(ctx.fake, "A1", ["peers-clear-std"], err)

      # Cycle 3 — A1 is :inactive; only `refresh_nodes/1` probes it.
      Fake.script_info_error(ctx.fake, "A1", ["partition-generation", "cluster-stable"], err)

      # Recovery cycle 4 — re-bootstrap + full healthy round.
      script_bootstrap_node(ctx.fake, "A1", 1, ReplicasFixture.all_master("test", 1))

      {:ok, pid} = start_tender(ctx, "test", failure_threshold: 2)

      :ok = Tender.tend_now(pid)
      assert Tender.ready?(pid)

      # Cycle 2 — flip to :inactive.
      :ok = Tender.tend_now(pid)
      assert %{"A1" => %{status: :inactive}} = Tender.nodes_status(pid)
      refute Tender.ready?(pid)

      # Cycle 3 — drop the node; state.nodes becomes empty.
      :ok = Tender.tend_now(pid)
      assert Tender.nodes_status(pid) == %{}

      # Cycle 4 — `bootstrap_if_needed/1` must re-enter because
      # state.nodes is empty. The replayed bootstrap cycle puts A1 back
      # into :active and re-applies the partition map.
      :ok = Tender.tend_now(pid)

      status = Tender.nodes_status(pid)
      assert %{"A1" => %{status: :active, generation_seen: 1}} = status
      assert Tender.ready?(pid)

      assert Fake.connect_count(ctx.fake, "A1") == 2,
             "seed must be re-dialled exactly once after the drop"
    end

    test "seed dial failure during recovery leaves state.nodes empty", ctx do
      # Same drop sequence as the previous test, but cycle 4 scripts a
      # `connect/3` failure against the seed — the recovery attempt must
      # fail cleanly, state.nodes must stay empty, and no partial node
      # entry is left behind. The next cycle (cycle 5) falls back to the
      # Fake's default success path and A1 finally comes back.
      script_bootstrap_node(ctx.fake, "A1", 1, ReplicasFixture.all_master("test", 1))

      err = %Error{code: :network_error, message: "injected"}

      Fake.script_info_error(ctx.fake, "A1", ["partition-generation", "cluster-stable"], err)
      Fake.script_info_error(ctx.fake, "A1", ["peers-clear-std"], err)

      Fake.script_info_error(ctx.fake, "A1", ["partition-generation", "cluster-stable"], err)

      {:ok, pid} = start_tender(ctx, "test", failure_threshold: 2)

      :ok = Tender.tend_now(pid)
      :ok = Tender.tend_now(pid)
      :ok = Tender.tend_now(pid)
      assert Tender.nodes_status(pid) == %{}

      # Script the recovery-cycle connect failure AFTER cycle 1 has
      # consumed its own successful connect so the error is queued to
      # be observed by the first re-bootstrap attempt, not the initial
      # bootstrap dial.
      Fake.script_connect(ctx.fake, "A1", {:error, err})

      # Cycle 4 — re-bootstrap dials the seed; the scripted failure
      # short-circuits and no node is registered.
      :ok = Tender.tend_now(pid)
      assert Tender.nodes_status(pid) == %{}
      refute Tender.ready?(pid)

      # Cycle 5 — retry. No more scripted connect outcomes remain so the
      # Fake falls back to the default success path. Script a fresh
      # healthy bootstrap cycle so the re-dial can install A1.
      script_bootstrap_node(ctx.fake, "A1", 1, ReplicasFixture.all_master("test", 1))

      :ok = Tender.tend_now(pid)
      assert %{"A1" => %{status: :active}} = Tender.nodes_status(pid)
      assert Tender.ready?(pid)
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

      script_bootstrap_info(ctx.fake, "A1")
      script_bootstrap_info(ctx.fake, "B1")

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

      script_bootstrap_info(ctx.fake, "A1")
      script_bootstrap_info(ctx.fake, "B1")

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

      script_bootstrap_info(ctx.fake, "A1")
      script_bootstrap_info(ctx.fake, "B1")

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

      script_bootstrap_info(ctx.fake, "A1")
      script_bootstrap_info(ctx.fake, "B1")

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
      script_bootstrap_info(ctx.fake, "A1")

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
      # in cycle 2 and is not restarted by the current recovery path.
      # The test pins the post-recovery counters field to nil so any
      # future change that re-introduces pool restart on recovery can
      # update this expectation in one place instead of discovering the
      # contract by accident.
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
      # Pool is not restarted during recovery (current behaviour), so
      # counters remain nil after recovery. The pre-recovery reference
      # must be gone — it is unsafe for anyone to still be writing
      # through it because the pool that produced those writes is dead.
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

  describe "node_handle/2 :use_compression gating" do
    test "cluster flag off → handle reports compression disabled even if node advertises it",
         ctx do
      {:ok, node_sup} = Aerospike.NodeSupervisor.start_link(name: ctx.name)
      on_exit(fn -> stop_process(node_sup) end)

      Fake.script_info(ctx.fake, "A1", ["node", "features"], %{
        "node" => "A1",
        "features" => "compression;pipelining"
      })

      script_cycle(ctx.fake, "A1",
        gen: 1,
        peers: "0,3000,[]",
        replicas: ReplicasFixture.all_master("test", 1)
      )

      {:ok, pid} =
        start_tender(ctx, "test",
          node_supervisor: Aerospike.NodeSupervisor.sup_name(ctx.name),
          pool_size: 1,
          use_compression: false
        )

      :ok = Tender.tend_now(pid)

      {:ok, handle} = Tender.node_handle(pid, "A1")
      assert handle.use_compression == false
    end

    test "cluster flag on + node lacks :compression feature → handle reports disabled", ctx do
      {:ok, node_sup} = Aerospike.NodeSupervisor.start_link(name: ctx.name)
      on_exit(fn -> stop_process(node_sup) end)

      # Node advertises pipelining but not compression.
      Fake.script_info(ctx.fake, "A1", ["node", "features"], %{
        "node" => "A1",
        "features" => "pipelining"
      })

      script_cycle(ctx.fake, "A1",
        gen: 1,
        peers: "0,3000,[]",
        replicas: ReplicasFixture.all_master("test", 1)
      )

      {:ok, pid} =
        start_tender(ctx, "test",
          node_supervisor: Aerospike.NodeSupervisor.sup_name(ctx.name),
          pool_size: 1,
          use_compression: true
        )

      :ok = Tender.tend_now(pid)

      {:ok, handle} = Tender.node_handle(pid, "A1")
      assert handle.use_compression == false
    end

    test "cluster flag on + node advertises :compression → handle reports enabled", ctx do
      {:ok, node_sup} = Aerospike.NodeSupervisor.start_link(name: ctx.name)
      on_exit(fn -> stop_process(node_sup) end)

      Fake.script_info(ctx.fake, "A1", ["node", "features"], %{
        "node" => "A1",
        "features" => "compression;pipelining"
      })

      script_cycle(ctx.fake, "A1",
        gen: 1,
        peers: "0,3000,[]",
        replicas: ReplicasFixture.all_master("test", 1)
      )

      {:ok, pid} =
        start_tender(ctx, "test",
          node_supervisor: Aerospike.NodeSupervisor.sup_name(ctx.name),
          pool_size: 1,
          use_compression: true
        )

      :ok = Tender.tend_now(pid)

      {:ok, handle} = Tender.node_handle(pid, "A1")
      assert handle.use_compression == true
    end

    test "default cluster flag is false", ctx do
      {:ok, node_sup} = Aerospike.NodeSupervisor.start_link(name: ctx.name)
      on_exit(fn -> stop_process(node_sup) end)

      Fake.script_info(ctx.fake, "A1", ["node", "features"], %{
        "node" => "A1",
        "features" => "compression"
      })

      script_cycle(ctx.fake, "A1",
        gen: 1,
        peers: "0,3000,[]",
        replicas: ReplicasFixture.all_master("test", 1)
      )

      {:ok, pid} =
        start_tender(ctx, "test",
          node_supervisor: Aerospike.NodeSupervisor.sup_name(ctx.name),
          pool_size: 1
        )

      :ok = Tender.tend_now(pid)

      {:ok, handle} = Tender.node_handle(pid, "A1")
      assert handle.use_compression == false
    end
  end

  describe ":use_services_alternate picks the peer-discovery info key" do
    test "default uses peers-clear-std: a std-only script produces a clean cycle", ctx do
      script_bootstrap_node(ctx.fake, "A1", 1, ReplicasFixture.all_master("test", 1))

      {:ok, pid} = start_tender(ctx, "test")
      :ok = Tender.tend_now(pid)

      # A successful peers reply clears the failure counter; if the
      # Tender had asked for a different info key the Fake's default
      # `:no_script` error would have bumped `failures` instead.
      assert Tender.nodes_status(pid)["A1"].failures == 0
      assert Tender.nodes_status(pid)["A1"].last_tend_result == :ok
    end

    test ":use_services_alternate true asks for peers-clear-alt, not peers-clear-std", ctx do
      # Seed handshake and the two non-peers cycle probes are scripted as
      # usual. Only the peers probe diverges.
      script_bootstrap_info(ctx.fake, "A1")

      Fake.script_info(ctx.fake, "A1", ["partition-generation", "cluster-stable"], %{
        "partition-generation" => "1",
        "cluster-stable" => "deadbeef"
      })

      Fake.script_info(ctx.fake, "A1", ["peers-clear-alt"], %{
        "peers-clear-alt" => "0,3000,[]"
      })

      Fake.script_info(ctx.fake, "A1", ["replicas"], %{
        "replicas" => ReplicasFixture.all_master("test", 1)
      })

      {:ok, pid} = start_tender(ctx, "test", use_services_alternate: true)
      :ok = Tender.tend_now(pid)

      assert Tender.nodes_status(pid)["A1"].failures == 0
      assert Tender.nodes_status(pid)["A1"].last_tend_result == :ok
      assert Tender.ready?(pid)
    end

    test ":use_services_alternate true with only a peers-clear-std script flips the node", ctx do
      # Scripting peers under the std key while the Tender asks for the
      # alt key makes the peers probe fall through the Fake's
      # `:no_script` default on every cycle. With `failure_threshold: 1`
      # the first breach is enough to flip A1 to `:inactive` — direct
      # evidence that the configured toggle selected the alternate key.
      script_bootstrap_info(ctx.fake, "A1")

      Fake.script_info(ctx.fake, "A1", ["partition-generation", "cluster-stable"], %{
        "partition-generation" => "1",
        "cluster-stable" => "deadbeef"
      })

      Fake.script_info(ctx.fake, "A1", ["peers-clear-std"], %{
        "peers-clear-std" => "0,3000,[]"
      })

      Fake.script_info(ctx.fake, "A1", ["replicas"], %{
        "replicas" => ReplicasFixture.all_master("test", 1)
      })

      {:ok, pid} =
        start_tender(ctx, "test", use_services_alternate: true, failure_threshold: 1)

      :ok = Tender.tend_now(pid)

      assert Tender.nodes_status(pid)["A1"].status == :inactive
    end

    test "either key produces identical peer node registrations", ctx do
      # Run the same peer-discovery scenario under both toggles and
      # compare the resulting node registrations: identical `features`
      # (empty MapSet), identical lifecycle status, identical generations.
      register_node(ctx.fake, "B1", "10.0.0.2", 3000)

      script_bootstrap_info(ctx.fake, "A1")
      script_bootstrap_info(ctx.fake, "B1")

      Fake.script_info(ctx.fake, "A1", ["partition-generation", "cluster-stable"], %{
        "partition-generation" => "1",
        "cluster-stable" => "deadbeef"
      })

      Fake.script_info(ctx.fake, "A1", ["peers-clear-alt"], %{
        "peers-clear-alt" => "1,3000,[[B1,,[10.0.0.2:3000]]]"
      })

      Fake.script_info(ctx.fake, "B1", ["features"], %{"features" => ""})

      Fake.script_info(ctx.fake, "A1", ["replicas"], %{
        "replicas" => ReplicasFixture.all_master("test", 1)
      })

      Fake.script_info(ctx.fake, "B1", ["replicas"], %{
        "replicas" => ReplicasFixture.all_master("test", 1)
      })

      {:ok, pid} = start_tender(ctx, "test", use_services_alternate: true)
      :ok = Tender.tend_now(pid)

      alt_snapshot = Tender.nodes_status(pid)

      assert Map.keys(alt_snapshot) |> Enum.sort() == ["A1", "B1"]
      assert alt_snapshot["B1"].status == :active
      assert alt_snapshot["B1"].features == MapSet.new()

      # Tear down and rerun the same flow under the std key. A fresh
      # Fake and Tender keep the two scenarios isolated.
      stop_tender(pid)
      stop_fake(ctx.fake)

      {:ok, fake2} = Fake.start_link(nodes: [{"A1", "10.0.0.1", 3000}, {"B1", "10.0.0.2", 3000}])
      ctx2 = %{ctx | fake: fake2, name: :"#{ctx.name}_std"}
      {:ok, owner2} = TableOwner.start_link(name: ctx2.name)
      tables2 = TableOwner.tables(owner2)
      ctx2 = %{ctx2 | owner: owner2, tables: tables2}

      on_exit(fn ->
        stop_fake(fake2)
        stop_process(owner2)
      end)

      script_bootstrap_info(ctx2.fake, "A1")
      script_bootstrap_info(ctx2.fake, "B1")

      Fake.script_info(ctx2.fake, "A1", ["partition-generation", "cluster-stable"], %{
        "partition-generation" => "1",
        "cluster-stable" => "deadbeef"
      })

      Fake.script_info(ctx2.fake, "A1", ["peers-clear-std"], %{
        "peers-clear-std" => "1,3000,[[B1,,[10.0.0.2:3000]]]"
      })

      Fake.script_info(ctx2.fake, "B1", ["features"], %{"features" => ""})

      Fake.script_info(ctx2.fake, "A1", ["replicas"], %{
        "replicas" => ReplicasFixture.all_master("test", 1)
      })

      Fake.script_info(ctx2.fake, "B1", ["replicas"], %{
        "replicas" => ReplicasFixture.all_master("test", 1)
      })

      {:ok, pid2} = start_tender(ctx2, "test")
      :ok = Tender.tend_now(pid2)

      std_snapshot = Tender.nodes_status(pid2)

      # The two snapshots match field-by-field for B1 on every stable
      # attribute. `last_tend_at` is monotonic-ms-based and will differ
      # between runs, so strip it before comparing. `:counters` and
      # `:tend_histogram` are `:atomics`/`:counters` references — fresh
      # per allocation so their terms differ across the two runs even
      # though their content is equivalent.
      assert Map.drop(alt_snapshot["B1"], [:last_tend_at, :counters, :tend_histogram]) ==
               Map.drop(std_snapshot["B1"], [:last_tend_at, :counters, :tend_histogram])
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

  describe "tend-cycle telemetry and per-node histogram" do
    test "tend_cycle span fires on every cycle with matching start/stop", ctx do
      script_bootstrap_node(ctx.fake, "A1", 1, ReplicasFixture.all_master("test", 1))

      {:ok, pid} = start_tender(ctx, "test")

      events = [
        [:aerospike, :tender, :tend_cycle, :start],
        [:aerospike, :tender, :tend_cycle, :stop]
      ]

      handler_id = attach_handler(events)
      on_exit(fn -> :telemetry.detach(handler_id) end)

      :ok = Tender.tend_now(pid)

      assert_receive {:event, [:aerospike, :tender, :tend_cycle, :start], start_meas, _},
                     1_000

      assert %{monotonic_time: _} = start_meas

      assert_receive {:event, [:aerospike, :tender, :tend_cycle, :stop], stop_meas, _},
                     1_000

      assert %{duration: duration} = stop_meas
      assert is_integer(duration)
      assert duration >= 0
    end

    test "partition_map_refresh span fires as a nested span inside tend_cycle", ctx do
      script_bootstrap_node(ctx.fake, "A1", 1, ReplicasFixture.all_master("test", 1))

      {:ok, pid} = start_tender(ctx, "test")

      events = [
        [:aerospike, :tender, :tend_cycle, :start],
        [:aerospike, :tender, :tend_cycle, :stop],
        [:aerospike, :tender, :partition_map_refresh, :start],
        [:aerospike, :tender, :partition_map_refresh, :stop]
      ]

      handler_id = attach_handler(events)
      on_exit(fn -> :telemetry.detach(handler_id) end)

      :ok = Tender.tend_now(pid)

      # Expected ordering: cycle_start, partition_start, partition_stop,
      # cycle_stop. Assert the nesting directly so a future refactor that
      # lifts the partition-map refresh out of the cycle would fail loudly.
      assert_receive {:event, [:aerospike, :tender, :tend_cycle, :start], _, _}, 1_000

      assert_receive {:event, [:aerospike, :tender, :partition_map_refresh, :start], _, _},
                     1_000

      assert_receive {:event, [:aerospike, :tender, :partition_map_refresh, :stop], _, _},
                     1_000

      assert_receive {:event, [:aerospike, :tender, :tend_cycle, :stop], _, _}, 1_000
    end

    test "a replicas fetch records one sample into the node's histogram", ctx do
      script_bootstrap_node(ctx.fake, "A1", 1, ReplicasFixture.all_master("test", 1))

      {:ok, pid} = start_tender(ctx, "test")
      :ok = Tender.tend_now(pid)

      {:ok, ref} = Tender.tend_histogram(pid, "A1")
      assert Aerospike.TendHistogram.count(ref) == 1
      assert is_integer(Aerospike.TendHistogram.percentile(ref, 0.5))
    end

    test "nodes_status/1 snapshot exposes the histogram reference", ctx do
      script_bootstrap_node(ctx.fake, "A1", 1, ReplicasFixture.all_master("test", 1))

      {:ok, pid} = start_tender(ctx, "test")
      :ok = Tender.tend_now(pid)

      snapshot = Tender.nodes_status(pid)
      assert %{tend_histogram: hist_ref} = snapshot["A1"]
      assert hist_ref != nil
      # Snapshot reference and accessor reference are the same :atomics term.
      {:ok, accessor_ref} = Tender.tend_histogram(pid, "A1")
      assert accessor_ref == hist_ref
    end

    test "tend_histogram/2 returns :unknown_node for inactive nodes", ctx do
      script_bootstrap_node(ctx.fake, "A1", 1, ReplicasFixture.all_master("test", 1))

      err = %Error{code: :network_error, message: "injected"}

      Fake.script_info_error(ctx.fake, "A1", ["partition-generation", "cluster-stable"], err)
      Fake.script_info_error(ctx.fake, "A1", ["peers-clear-std"], err)

      {:ok, pid} = start_tender(ctx, "test", failure_threshold: 1)

      :ok = Tender.tend_now(pid)
      assert Tender.nodes_status(pid)["A1"].status == :active

      # Second cycle crosses the threshold and flips the node to :inactive.
      :ok = Tender.tend_now(pid)

      assert Tender.nodes_status(pid)["A1"].status == :inactive
      assert {:error, :unknown_node} = Tender.tend_histogram(pid, "A1")
    end

    test "tend_histogram/2 returns :unknown_node for an unknown node", ctx do
      script_bootstrap_node(ctx.fake, "A1", 1, ReplicasFixture.all_master("test", 1))
      {:ok, pid} = start_tender(ctx, "test")
      :ok = Tender.tend_now(pid)

      assert {:error, :unknown_node} = Tender.tend_histogram(pid, "nope")
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
      |> maybe_put(:use_compression, Keyword.get(opts, :use_compression))
      |> maybe_put(:use_services_alternate, Keyword.get(opts, :use_services_alternate))

    {:ok, pid} = Tender.start_link(tender_opts)

    on_exit(fn -> stop_tender(pid) end)
    {:ok, pid}
  end

  defp maybe_put(opts, _key, nil), do: opts
  defp maybe_put(opts, key, value), do: Keyword.put(opts, key, value)

  defp attach_handler(events) do
    handler_id = {__MODULE__, make_ref()}
    :ok = :telemetry.attach_many(handler_id, events, &__MODULE__.forward/4, self())
    handler_id
  end

  defp script_bootstrap_node(fake, node_name, partition_gen, replicas_value) do
    script_bootstrap_info(fake, node_name)

    script_cycle(fake, node_name,
      gen: partition_gen,
      peers: "0,3000,[]",
      replicas: replicas_value
    )
  end

  defp script_bootstrap_info(fake, node_name, features \\ "") do
    Fake.script_info(fake, node_name, ["node", "features"], %{
      "node" => node_name,
      "features" => features
    })
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

      # GenServer.stop can still race a concurrent exit (the Fake's
      # `on_exit` may tear down the linked Tender between the
      # `Process.alive?/1` check and this call); absorb :exit the same
      # way `stop_process/1` does.
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
