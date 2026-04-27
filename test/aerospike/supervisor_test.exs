defmodule Aerospike.Cluster.SupervisorTest do
  use ExUnit.Case, async: true

  alias Aerospike.Cluster
  alias Aerospike.Cluster.NodeSupervisor
  alias Aerospike.Cluster.PartitionMap
  alias Aerospike.Cluster.PartitionMapWriter
  alias Aerospike.Cluster.Supervisor, as: ClusterSupervisor
  alias Aerospike.Cluster.TableOwner
  alias Aerospike.Cluster.Tender
  alias Aerospike.Test.ReplicasFixture
  alias Aerospike.Transport.Fake

  setup context do
    name = :"sup_#{:erlang.phash2(context.test)}"

    {:ok, fake} = Fake.start_link(nodes: [{"A1", "10.0.0.1", 3000}])

    on_exit(fn -> stop_if_alive(fake) end)

    %{name: name, fake: fake}
  end

  describe "start_link/1 validation" do
    test "requires :name, :transport, :hosts, :namespaces" do
      assert_raise ArgumentError, ~r/missing required option :name/, fn ->
        ClusterSupervisor.start_link(
          transport: Fake,
          hosts: ["10.0.0.1:3000"],
          namespaces: ["test"]
        )
      end

      assert_raise ArgumentError, ~r/missing required option :transport/, fn ->
        ClusterSupervisor.start_link(
          name: :x,
          hosts: ["10.0.0.1:3000"],
          namespaces: ["test"]
        )
      end

      assert_raise ArgumentError, ~r/missing required option :hosts/, fn ->
        ClusterSupervisor.start_link(name: :x, transport: Fake, namespaces: ["test"])
      end

      assert_raise ArgumentError, ~r/missing required option :namespaces/, fn ->
        ClusterSupervisor.start_link(name: :x, transport: Fake, hosts: ["10.0.0.1:3000"])
      end
    end

    test "parses :hosts and passes tuple seeds to the tender" do
      assert {:ok, sup} =
               ClusterSupervisor.start_link(
                 name: :x,
                 transport: Fake,
                 hosts: ["10.0.0.1:3000", "10.0.0.2"],
                 namespaces: ["test"],
                 tend_trigger: :manual
               )

      Process.unlink(sup)

      assert %{
               seeds: [{"10.0.0.1", 3000}, {"10.0.0.2", 3000}]
             } = :sys.get_state(:x)

      stop_if_alive(sup)
    end

    test "rejects malformed :hosts entries" do
      assert_raise ArgumentError, ~r/invalid host port/, fn ->
        ClusterSupervisor.start_link(
          name: :x,
          transport: Fake,
          hosts: ["10.0.0.1:notaport"],
          namespaces: ["test"]
        )
      end
    end

    test "rejects non-atom :name" do
      assert_raise ArgumentError, ~r/:name must be an atom/, fn ->
        ClusterSupervisor.start_link(
          name: "not_an_atom",
          transport: Fake,
          hosts: ["10.0.0.1:3000"],
          namespaces: ["test"]
        )
      end
    end

    test "rejects empty :hosts" do
      assert_raise ArgumentError, ~r/:hosts must be a non-empty list/, fn ->
        ClusterSupervisor.start_link(
          name: :x,
          transport: Fake,
          hosts: [],
          namespaces: ["test"]
        )
      end
    end

    test "rejects malformed hosts" do
      assert_raise ArgumentError, ~r/each host must be a non-empty string/, fn ->
        ClusterSupervisor.start_link(
          name: :x,
          transport: Fake,
          hosts: [:bad_seed],
          namespaces: ["test"]
        )
      end

      assert_raise ArgumentError, ~r/invalid host port/, fn ->
        ClusterSupervisor.start_link(
          name: :x,
          transport: Fake,
          hosts: ["10.0.0.1:0"],
          namespaces: ["test"]
        )
      end
    end

    test "rejects empty :namespaces" do
      assert_raise ArgumentError, ~r/:namespaces must be a non-empty list/, fn ->
        ClusterSupervisor.start_link(
          name: :x,
          transport: Fake,
          hosts: ["10.0.0.1:3000"],
          namespaces: []
        )
      end
    end

    test "rejects non-string namespaces" do
      assert_raise ArgumentError, ~r/each namespace must be a non-empty string/, fn ->
        ClusterSupervisor.start_link(
          name: :x,
          transport: Fake,
          hosts: ["10.0.0.1:3000"],
          namespaces: [:test]
        )
      end
    end

    # Pool-level and TCP-level knob validation. Bad values should fail
    # synchronously at `start_link/1` — waiting for the first pool
    # worker's connect-failure to surface a misconfigured opt is
    # operator-hostile.
    test "rejects non-positive :pool_size" do
      assert_raise ArgumentError, ~r/:pool_size must be a positive integer/, fn ->
        ClusterSupervisor.start_link(
          name: :x,
          transport: Fake,
          hosts: ["10.0.0.1:3000"],
          namespaces: ["test"],
          pool_size: 0
        )
      end

      assert_raise ArgumentError,
                   ~r/:min_connections_per_node must be less than or equal to :pool_size/,
                   fn ->
                     ClusterSupervisor.start_link(
                       name: :x,
                       transport: Fake,
                       hosts: ["10.0.0.1:3000"],
                       namespaces: ["test"],
                       pool_size: 2,
                       min_connections_per_node: 3
                     )
                   end
    end

    test "rejects non-positive :idle_timeout_ms" do
      assert_raise ArgumentError, ~r/:idle_timeout_ms must be a positive integer/, fn ->
        ClusterSupervisor.start_link(
          name: :x,
          transport: Fake,
          hosts: ["10.0.0.1:3000"],
          namespaces: ["test"],
          idle_timeout_ms: -1
        )
      end
    end

    test "rejects non-positive :max_idle_pings" do
      assert_raise ArgumentError, ~r/:max_idle_pings must be a positive integer/, fn ->
        ClusterSupervisor.start_link(
          name: :x,
          transport: Fake,
          hosts: ["10.0.0.1:3000"],
          namespaces: ["test"],
          max_idle_pings: 0
        )
      end
    end

    test "rejects non-boolean connect_opts :tcp_nodelay" do
      assert_raise ArgumentError, ~r/:tcp_nodelay must be a boolean/, fn ->
        ClusterSupervisor.start_link(
          name: :x,
          transport: Fake,
          hosts: ["10.0.0.1:3000"],
          namespaces: ["test"],
          connect_opts: [tcp_nodelay: :yes]
        )
      end
    end

    test "rejects non-positive connect_opts :tcp_sndbuf" do
      assert_raise ArgumentError, ~r/:tcp_sndbuf must be a positive integer/, fn ->
        ClusterSupervisor.start_link(
          name: :x,
          transport: Fake,
          hosts: ["10.0.0.1:3000"],
          namespaces: ["test"],
          connect_opts: [tcp_sndbuf: 0]
        )
      end
    end

    test "rejects non-keyword :connect_opts" do
      assert_raise ArgumentError, ~r/:connect_opts must be a keyword list/, fn ->
        ClusterSupervisor.start_link(
          name: :x,
          transport: Fake,
          hosts: ["10.0.0.1:3000"],
          namespaces: ["test"],
          connect_opts: :not_a_list
        )
      end
    end

    test "rejects a partial auth pair" do
      assert_raise ArgumentError,
                   ~r/:user and :password must both be strings or both be absent/,
                   fn ->
                     ClusterSupervisor.start_link(
                       name: :x,
                       transport: Fake,
                       hosts: ["10.0.0.1:3000"],
                       namespaces: ["test"],
                       user: "admin"
                     )
                   end
    end

    test "validates startup auth mode shape" do
      assert_raise ArgumentError, ~r/:auth_mode must be :internal, :external, or :pki/, fn ->
        ClusterSupervisor.start_link(
          name: :x,
          transport: Fake,
          hosts: ["10.0.0.1:3000"],
          namespaces: ["test"],
          auth_mode: :ldap
        )
      end

      assert_raise ArgumentError, ~r/:external auth requires Aerospike.Transport.Tls/, fn ->
        ClusterSupervisor.start_link(
          name: :x,
          transport: Fake,
          hosts: ["10.0.0.1:3000"],
          namespaces: ["test"],
          auth_mode: :external,
          user: "admin",
          password: "secret"
        )
      end

      assert_raise ArgumentError, ~r/:pki auth uses the TLS client certificate/, fn ->
        ClusterSupervisor.start_link(
          name: :x,
          transport: Aerospike.Transport.Tls,
          hosts: ["10.0.0.1:3000"],
          namespaces: ["test"],
          auth_mode: :pki,
          user: "admin",
          password: "secret"
        )
      end
    end

    test "validates accepted startup discovery fields" do
      assert_raise ArgumentError, ~r/:cluster_name must be a non-empty string/, fn ->
        ClusterSupervisor.start_link(
          name: :x,
          transport: Fake,
          hosts: ["10.0.0.1:3000"],
          namespaces: ["test"],
          cluster_name: ""
        )
      end

      assert_raise ArgumentError, ~r/:application_id must be a non-empty string/, fn ->
        ClusterSupervisor.start_link(
          name: :x,
          transport: Fake,
          hosts: ["10.0.0.1:3000"],
          namespaces: ["test"],
          application_id: ""
        )
      end

      assert_raise ArgumentError, ~r/:seed_only_cluster must be a boolean/, fn ->
        ClusterSupervisor.start_link(
          name: :x,
          transport: Fake,
          hosts: ["10.0.0.1:3000"],
          namespaces: ["test"],
          seed_only_cluster: :yes
        )
      end
    end

    test "rejects invalid retry opts" do
      assert_raise ArgumentError, ~r/:max_retries must be a non-negative integer/, fn ->
        ClusterSupervisor.start_link(
          name: :x,
          transport: Fake,
          hosts: ["10.0.0.1:3000"],
          namespaces: ["test"],
          max_retries: -1
        )
      end

      assert_raise ArgumentError, ~r/:replica_policy must be :master or :sequence/, fn ->
        ClusterSupervisor.start_link(
          name: :x,
          transport: Fake,
          hosts: ["10.0.0.1:3000"],
          namespaces: ["test"],
          replica_policy: :any
        )
      end
    end

    test "rejects invalid breaker and lifecycle opts" do
      assert_raise ArgumentError,
                   ~r/:circuit_open_threshold must be a non-negative integer/,
                   fn ->
                     ClusterSupervisor.start_link(
                       name: :x,
                       transport: Fake,
                       hosts: ["10.0.0.1:3000"],
                       namespaces: ["test"],
                       circuit_open_threshold: -1
                     )
                   end

      assert_raise ArgumentError,
                   ~r/:max_concurrent_ops_per_node must be a positive integer/,
                   fn ->
                     ClusterSupervisor.start_link(
                       name: :x,
                       transport: Fake,
                       hosts: ["10.0.0.1:3000"],
                       namespaces: ["test"],
                       max_concurrent_ops_per_node: 0
                     )
                   end

      assert_raise ArgumentError, ~r/:tend_trigger must be :timer or :manual/, fn ->
        ClusterSupervisor.start_link(
          name: :x,
          transport: Fake,
          hosts: ["10.0.0.1:3000"],
          namespaces: ["test"],
          tend_trigger: :now
        )
      end
    end

    test "rejects non-boolean feature toggles" do
      assert_raise ArgumentError, ~r/:use_compression must be a boolean/, fn ->
        ClusterSupervisor.start_link(
          name: :x,
          transport: Fake,
          hosts: ["10.0.0.1:3000"],
          namespaces: ["test"],
          use_compression: :yes
        )
      end

      assert_raise ArgumentError, ~r/:use_services_alternate must be a boolean/, fn ->
        ClusterSupervisor.start_link(
          name: :x,
          transport: Fake,
          hosts: ["10.0.0.1:3000"],
          namespaces: ["test"],
          use_services_alternate: :alt
        )
      end
    end
  end

  describe "supervision tree shape" do
    test "starts TableOwner, NodeSupervisor, PartitionMapWriter, and Tender under one supervisor",
         ctx do
      {:ok, sup} = start_supervisor(ctx)

      assert Process.alive?(sup)
      assert Process.whereis(ClusterSupervisor.sup_name(ctx.name)) == sup

      assert Process.alive?(Process.whereis(TableOwner.via(ctx.name)))
      assert Process.alive?(Process.whereis(NodeSupervisor.sup_name(ctx.name)))
      assert Process.alive?(Process.whereis(PartitionMapWriter.via(ctx.name)))
      assert Process.alive?(Process.whereis(ctx.name))

      ids =
        sup
        |> Supervisor.which_children()
        |> Enum.map(fn {id, _pid, _type, _mods} -> id end)

      assert {TableOwner, ctx.name} in ids
      assert {NodeSupervisor, ctx.name} in ids
      assert {PartitionMapWriter, ctx.name} in ids
      assert {Tender, ctx.name} in ids
    end

    test "Tender can run a tend cycle (TableOwner's tables are reachable)", ctx do
      script_bootstrap_node(ctx.fake, "A1", 1, ReplicasFixture.all_master("test", 1))

      {:ok, _sup} = start_supervisor(ctx)

      refute Cluster.ready?(ctx.name)
      :ok = Tender.tend_now(ctx.name)
      assert Cluster.ready?(ctx.name)

      %{owners: owners} = Cluster.tables(ctx.name)
      {:ok, po} = PartitionMap.owners(owners, "test", 0)
      assert po.replicas == ["A1"]
    end
  end

  describe "rest_for_one crash semantics" do
    test "killing the Tender restarts it while TableOwner keeps the ETS state", ctx do
      script_bootstrap_node(ctx.fake, "A1", 1, ReplicasFixture.all_master("test", 1))

      {:ok, _sup} = start_supervisor(ctx)
      tender_before = Process.whereis(ctx.name)
      owner_before = Process.whereis(TableOwner.via(ctx.name))
      node_sup_before = Process.whereis(NodeSupervisor.sup_name(ctx.name))
      writer_before = Process.whereis(PartitionMapWriter.via(ctx.name))

      :ok = Tender.tend_now(ctx.name)
      tables_before = Cluster.tables(ctx.name)
      {:ok, po_before} = PartitionMap.owners(tables_before.owners, "test", 0)
      assert Cluster.ready?(ctx.name)

      Process.exit(tender_before, :kill)
      await_replaced(ctx.name, tender_before)

      tender_after = Process.whereis(ctx.name)
      assert tender_after != tender_before
      assert Process.whereis(TableOwner.via(ctx.name)) == owner_before
      assert Process.whereis(NodeSupervisor.sup_name(ctx.name)) == node_sup_before
      assert Process.whereis(PartitionMapWriter.via(ctx.name)) == writer_before

      # TableOwner kept the rows the previous Tender wrote — the new
      # Tender exposes the exact same tables and the :ready meta flag
      # survives the restart.
      assert Cluster.tables(ctx.name) == tables_before
      {:ok, po_after} = PartitionMap.owners(tables_before.owners, "test", 0)
      assert po_after == po_before
      assert Cluster.ready?(ctx.name)
    end

    test "killing TableOwner restarts the whole subtree with fresh tables", ctx do
      script_bootstrap_node(ctx.fake, "A1", 1, ReplicasFixture.all_master("test", 1))

      {:ok, _sup} = start_supervisor(ctx)
      :ok = Tender.tend_now(ctx.name)
      assert Cluster.ready?(ctx.name)

      tender_before = Process.whereis(ctx.name)
      owner_before = Process.whereis(TableOwner.via(ctx.name))
      node_sup_before = Process.whereis(NodeSupervisor.sup_name(ctx.name))

      Process.exit(owner_before, :kill)
      await_replaced(ctx.name, tender_before)

      owner_after = Process.whereis(TableOwner.via(ctx.name))
      tender_after = Process.whereis(ctx.name)
      node_sup_after = Process.whereis(NodeSupervisor.sup_name(ctx.name))

      # Under rest_for_one, siblings started after TableOwner also
      # restart, so NodeSupervisor and Tender get fresh pids too.
      assert owner_after != owner_before
      assert tender_after != tender_before
      assert node_sup_after != node_sup_before

      # Fresh TableOwner means a fresh :meta row — ready? is back to
      # false until the replacement Tender tends again.
      refute Cluster.ready?(ctx.name)
    end
  end

  ## Helpers

  defp start_supervisor(ctx, opts \\ []) do
    sup_opts =
      [
        name: ctx.name,
        transport: Fake,
        connect_opts: [fake: ctx.fake],
        hosts: Keyword.get(opts, :hosts, ["10.0.0.1:3000"]),
        namespaces: Keyword.get(opts, :namespaces, ["test"]),
        tend_trigger: :manual
      ]

    {:ok, sup} = ClusterSupervisor.start_link(sup_opts)

    on_exit(fn -> stop_if_alive(sup) end)
    {:ok, sup}
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

  defp await_replaced(name, old_pid, attempts \\ 50) do
    case Process.whereis(name) do
      pid when is_pid(pid) and pid != old_pid ->
        pid

      _ when attempts > 0 ->
        Process.sleep(10)
        await_replaced(name, old_pid, attempts - 1)

      _ ->
        flunk("timed out waiting for #{inspect(name)} to be replaced")
    end
  end

  defp stop_if_alive(pid) when is_pid(pid) do
    if Process.alive?(pid) do
      ref = Process.monitor(pid)
      Process.exit(pid, :shutdown)

      receive do
        {:DOWN, ^ref, _, _, _} -> :ok
      after
        2_000 -> :ok
      end
    end
  end

  defp stop_if_alive(_), do: :ok
end
