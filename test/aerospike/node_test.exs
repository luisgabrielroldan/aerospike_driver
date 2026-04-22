defmodule Aerospike.NodeTest do
  @moduledoc """
  Seam tests for `Aerospike.Node`. Exercise the per-node info-socket
  operations against `Aerospike.Transport.Fake` with scripted replies,
  covering the failure paths the tend cycle relies on: transport errors,
  malformed `partition-generation`, missing `cluster-stable`, empty or
  malformed peers replies, and the feature-probe absent-key fallback.
  """

  use ExUnit.Case, async: true

  alias Aerospike.Error
  alias Aerospike.Node
  alias Aerospike.Test.ReplicasFixture
  alias Aerospike.Transport.Fake

  setup do
    {:ok, fake} = Fake.start_link(nodes: [{"A1", "10.0.0.1", 3000}])
    on_exit(fn -> stop_if_alive(fake) end)
    %{fake: fake}
  end

  describe "seed/5" do
    test "returns a %Node{} with parsed features on success", %{fake: fake} do
      Fake.script_info(fake, "A1", ["node", "features"], %{
        "node" => "A1",
        "features" => "compression;pipelining"
      })

      {:ok, node} = Node.seed(Fake, "10.0.0.1", 3000, [fake: fake], user: nil, password: nil)

      assert %Node{name: "A1", host: "10.0.0.1", port: 3000, session: nil} = node
      assert MapSet.member?(node.features, :compression)
      assert MapSet.member?(node.features, :pipelining)
      assert node.generation_seen == nil
      assert node.applied_gen == nil
      assert node.cluster_stable == nil
      assert node.peers_generation_seen == nil
    end

    test "missing features key collapses to empty MapSet", %{fake: fake} do
      Fake.script_info(fake, "A1", ["node", "features"], %{"node" => "A1"})

      {:ok, node} = Node.seed(Fake, "10.0.0.1", 3000, [fake: fake], user: nil, password: nil)

      assert node.features == MapSet.new()
    end

    test "connect failure surfaces verbatim", %{fake: fake} do
      err = %Error{code: :connection_error, message: "refused"}
      Fake.script_connect(fake, "A1", {:error, err})

      assert {:error, ^err} =
               Node.seed(Fake, "10.0.0.1", 3000, [fake: fake], user: nil, password: nil)
    end

    test "info reply missing `node` key returns :no_node_info", %{fake: fake} do
      # Reply is a success shape but without `node`; Node.seed pattern match
      # sends it into the `{:ok, _other}` fallback.
      Fake.script_info(fake, "A1", ["node", "features"], %{"features" => "compression"})

      assert {:error, :no_node_info} =
               Node.seed(Fake, "10.0.0.1", 3000, [fake: fake], user: nil, password: nil)
    end

    test "info transport error surfaces as Error struct", %{fake: fake} do
      err = %Error{code: :network_error, message: "injected"}
      Fake.script_info_error(fake, "A1", ["node", "features"], err)

      assert {:error, ^err} =
               Node.seed(Fake, "10.0.0.1", 3000, [fake: fake], user: nil, password: nil)
    end
  end

  describe "refresh/2" do
    test "returns updated struct and observations on a complete reply", %{fake: fake} do
      node = connected_node(fake, "A1")

      Fake.script_info(
        fake,
        "A1",
        ["partition-generation", "cluster-stable", "peers-generation"],
        %{
          "partition-generation" => "7",
          "cluster-stable" => "deadbeef",
          "peers-generation" => "1"
        }
      )

      {:ok, updated, obs} = Node.refresh(node, Fake)

      assert updated.generation_seen == 7
      assert updated.cluster_stable == "deadbeef"
      assert updated.peers_generation_seen == 1

      assert obs == %{
               partition_generation: 7,
               cluster_stable: "deadbeef",
               peers_generation: 1,
               peers_generation_changed?: true
             }
    end

    test "malformed partition-generation is flagged as :malformed_reply", %{fake: fake} do
      node = connected_node(fake, "A1")

      Fake.script_info(
        fake,
        "A1",
        ["partition-generation", "cluster-stable", "peers-generation"],
        %{
          "partition-generation" => "not-a-number",
          "cluster-stable" => "deadbeef",
          "peers-generation" => "1"
        }
      )

      assert {:error, :malformed_reply} = Node.refresh(node, Fake)
    end

    test "missing cluster-stable is rejected even with a good generation", %{fake: fake} do
      node = connected_node(fake, "A1")

      Fake.script_info(
        fake,
        "A1",
        ["partition-generation", "cluster-stable", "peers-generation"],
        %{
          "partition-generation" => "3",
          "cluster-stable" => "",
          "peers-generation" => "1"
        }
      )

      assert {:error, :malformed_reply} = Node.refresh(node, Fake)
    end

    test "transport failure propagates the Error struct untouched", %{fake: fake} do
      node = connected_node(fake, "A1")
      err = %Error{code: :network_error, message: "injected"}

      Fake.script_info_error(
        fake,
        "A1",
        ["partition-generation", "cluster-stable", "peers-generation"],
        err
      )

      assert {:error, ^err} = Node.refresh(node, Fake)
    end

    test "the struct is left untouched on malformed replies", %{fake: fake} do
      node = connected_node(fake, "A1")

      Fake.script_info(
        fake,
        "A1",
        ["partition-generation", "cluster-stable", "peers-generation"],
        %{
          "partition-generation" => "bogus",
          "cluster-stable" => "deadbeef",
          "peers-generation" => "1"
        }
      )

      {:error, :malformed_reply} = Node.refresh(node, Fake)

      # Re-run against a healthy script to confirm the node was never
      # mutated in place — fresh replies produce fresh observations.
      Fake.script_info(
        fake,
        "A1",
        ["partition-generation", "cluster-stable", "peers-generation"],
        %{
          "partition-generation" => "1",
          "cluster-stable" => "cafef00d",
          "peers-generation" => "1"
        }
      )

      {:ok, updated, _obs} = Node.refresh(node, Fake)
      assert updated.generation_seen == 1
      assert updated.cluster_stable == "cafef00d"
    end

    test "peers_generation_changed? flips to false when the cached value matches",
         %{fake: fake} do
      node = connected_node(fake, "A1")

      Enum.each(1..2, fn _ ->
        Fake.script_info(
          fake,
          "A1",
          ["partition-generation", "cluster-stable", "peers-generation"],
          %{
            "partition-generation" => "1",
            "cluster-stable" => "deadbeef",
            "peers-generation" => "42"
          }
        )
      end)

      {:ok, updated, obs1} = Node.refresh(node, Fake)
      assert obs1.peers_generation == 42
      assert obs1.peers_generation_changed? == true
      assert updated.peers_generation_seen == 42

      {:ok, _updated2, obs2} = Node.refresh(updated, Fake)
      assert obs2.peers_generation == 42
      assert obs2.peers_generation_changed? == false
    end
  end

  describe "refresh_peers/3" do
    test "returns the list of peers parsed from peers-clear-std", %{fake: fake} do
      node = connected_node(fake, "A1")

      Fake.script_info(fake, "A1", ["peers-clear-std"], %{
        "peers-clear-std" => "1,3000,[[B1,,[10.0.0.2:3000]]]"
      })

      {:ok, ^node, peers} = Node.refresh_peers(node, Fake)

      assert [%{node_name: "B1", host: "10.0.0.2", port: 3000}] = peers
    end

    test "empty peer list comes back as []", %{fake: fake} do
      node = connected_node(fake, "A1")

      Fake.script_info(fake, "A1", ["peers-clear-std"], %{
        "peers-clear-std" => "0,3000,[]"
      })

      {:ok, _node, peers} = Node.refresh_peers(node, Fake)
      assert peers == []
    end

    test "picks the alternate info key when asked", %{fake: fake} do
      node = connected_node(fake, "A1")

      Fake.script_info(fake, "A1", ["peers-clear-alt"], %{
        "peers-clear-alt" => "0,3000,[]"
      })

      {:ok, _node, peers} = Node.refresh_peers(node, Fake, use_services_alternate: true)
      assert peers == []
    end

    test "malformed peers body is flagged as :malformed_reply", %{fake: fake} do
      node = connected_node(fake, "A1")

      Fake.script_info(fake, "A1", ["peers-clear-std"], %{
        "peers-clear-std" => "not-a-peers-value"
      })

      assert {:error, :malformed_reply} = Node.refresh_peers(node, Fake)
    end

    test "transport failure propagates", %{fake: fake} do
      node = connected_node(fake, "A1")
      err = %Error{code: :network_error, message: "injected"}
      Fake.script_info_error(fake, "A1", ["peers-clear-std"], err)

      assert {:error, ^err} = Node.refresh_peers(node, Fake)
    end
  end

  describe "refresh_partitions/2" do
    test "returns parsed segments alongside the struct", %{fake: fake} do
      node = connected_node(fake, "A1")
      replicas = ReplicasFixture.all_master("test", 5)

      Fake.script_info(fake, "A1", ["replicas"], %{"replicas" => replicas})

      {:ok, ^node, segments} = Node.refresh_partitions(node, Fake)

      # Exactly one namespace segment — `test` — at regime 5 with a single
      # replica slot covering every partition.
      assert [{"test", 5, ownership}] = segments
      assert length(ownership) == 4096
      assert Enum.all?(ownership, fn {_pid, replica_index} -> replica_index == 0 end)
    end

    test "transport failure propagates the Error struct", %{fake: fake} do
      node = connected_node(fake, "A1")
      err = %Error{code: :network_error, message: "injected"}
      Fake.script_info_error(fake, "A1", ["replicas"], err)

      assert {:error, ^err} = Node.refresh_partitions(node, Fake)
    end

    test "reply missing `replicas` key is flagged as :malformed_reply", %{fake: fake} do
      node = connected_node(fake, "A1")

      Fake.script_info(fake, "A1", ["replicas"], %{"other" => "ignored"})

      assert {:error, :malformed_reply} = Node.refresh_partitions(node, Fake)
    end
  end

  describe "fetch_features/3" do
    test "probe failure collapses to the empty set", %{fake: fake} do
      {:ok, conn} = Fake.connect("10.0.0.1", 3000, fake: fake)
      err = %Error{code: :network_error, message: "injected"}
      Fake.script_info_error(fake, "A1", ["features"], err)

      assert MapSet.new() == Node.fetch_features(Fake, conn, "A1")
    end

    test "recognised tokens come back as atoms, unknowns as {:unknown, raw}", %{fake: fake} do
      {:ok, conn} = Fake.connect("10.0.0.1", 3000, fake: fake)

      Fake.script_info(fake, "A1", ["features"], %{
        "features" => "compression;batch-index"
      })

      features = Node.fetch_features(Fake, conn, "A1")
      assert MapSet.member?(features, :compression)
      assert MapSet.member?(features, {:unknown, "batch-index"})
    end
  end

  describe "mark_partition_map_applied/2" do
    test "accepts nil so freshly-seen peers can clear the first refetch cycle" do
      node = sample_node()
      assert %Node{applied_gen: nil} = Node.mark_partition_map_applied(node, nil)
    end

    test "records the generation for later comparisons" do
      node = sample_node()
      assert %Node{applied_gen: 42} = Node.mark_partition_map_applied(node, 42)
    end
  end

  describe "clear_cluster_stable/1" do
    test "zeroes just the cluster-stable hash" do
      node = %{sample_node() | cluster_stable: "deadbeef", generation_seen: 3}
      cleared = Node.clear_cluster_stable(node)

      assert cleared.cluster_stable == nil
      assert cleared.generation_seen == 3
    end
  end

  describe "close/2" do
    test "delegates to transport.close and swallows exits" do
      # Pass a nonsense conn — the fake's real close/1 would crash on it.
      # `close/2` must swallow any raised or exited error so lifecycle
      # callers can invoke it unconditionally.
      assert :ok = Node.close(CrashingTransport, :not_a_conn)
    end
  end

  defmodule CrashingTransport do
    @moduledoc false
    def close(_), do: raise("boom")
  end

  ## Helpers

  defp connected_node(fake, name) do
    {:ok, conn} = Fake.connect("10.0.0.1", 3000, fake: fake)

    %Node{name: name, host: "10.0.0.1", port: 3000, conn: conn}
  end

  defp sample_node do
    %Node{name: "A1", host: "10.0.0.1", port: 3000, conn: :fake_conn}
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
end
