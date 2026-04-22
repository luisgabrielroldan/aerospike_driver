defmodule Aerospike.PartitionMapWriterTest do
  @moduledoc """
  Seam tests for `Aerospike.PartitionMapWriter`. Each public call is
  exercised against a freshly-minted `Aerospike.TableOwner`-backed set of
  ETS tables; the tests assert the ETS effect rather than the writer's
  trivial internal state. Crash semantics are verified by placing the
  writer under a dedicated supervisor and asserting restart behaviour.
  """

  use ExUnit.Case, async: true

  alias Aerospike.PartitionMap
  alias Aerospike.PartitionMapWriter
  alias Aerospike.RetryPolicy
  alias Aerospike.TableOwner

  setup context do
    name = :"writer_#{:erlang.phash2(context.test)}"

    {:ok, owner} = TableOwner.start_link(name: name)
    tables = TableOwner.tables(owner)

    {:ok, writer} = PartitionMapWriter.start_link(name: name, tables: tables)

    on_exit(fn ->
      stop_if_alive(writer)
      stop_if_alive(owner)
    end)

    %{name: name, owner: owner, writer: writer, tables: tables}
  end

  describe "apply_segments/4" do
    test "writes to the owners table and returns true on clean acceptance", ctx do
      segments = [{"test", 1, [{0, 0}, {4095, 0}]}]

      assert true ==
               PartitionMapWriter.apply_segments(ctx.writer, "A1", segments, ["test"])

      {:ok, po} = PartitionMap.owners(ctx.tables.owners, "test", 0)
      assert po.regime == 1
      assert po.replicas == ["A1"]
    end

    test "returns false when any segment is stale", ctx do
      fresh = [{"test", 5, [{0, 0}]}]
      stale = [{"test", 4, [{0, 0}]}]

      assert PartitionMapWriter.apply_segments(ctx.writer, "A1", fresh, ["test"])
      refute PartitionMapWriter.apply_segments(ctx.writer, "A1", stale, ["test"])

      {:ok, po} = PartitionMap.owners(ctx.tables.owners, "test", 0)
      assert po.regime == 5
    end

    test "an out-of-scope namespace is skipped without writing", ctx do
      segments = [{"other", 1, [{0, 0}]}]

      assert PartitionMapWriter.apply_segments(ctx.writer, "A1", segments, ["test"])

      assert {:error, :unknown_partition} =
               PartitionMap.owners(ctx.tables.owners, "other", 0)
    end
  end

  describe "put_node_gen/3 and delete_node_gen/2" do
    test "put_node_gen writes the generation for the node", ctx do
      assert :ok = PartitionMapWriter.put_node_gen(ctx.writer, "A1", 7)
      assert {:ok, 7} = PartitionMap.get_node_gen(ctx.tables.node_gens, "A1")
    end

    test "put_node_gen overwrites on subsequent calls", ctx do
      :ok = PartitionMapWriter.put_node_gen(ctx.writer, "A1", 7)
      :ok = PartitionMapWriter.put_node_gen(ctx.writer, "A1", 12)

      assert {:ok, 12} = PartitionMap.get_node_gen(ctx.tables.node_gens, "A1")
    end

    test "delete_node_gen removes the row", ctx do
      :ok = PartitionMapWriter.put_node_gen(ctx.writer, "A1", 7)
      assert :ok = PartitionMapWriter.delete_node_gen(ctx.writer, "A1")
      assert {:error, :unknown_node} = PartitionMap.get_node_gen(ctx.tables.node_gens, "A1")
    end

    test "delete_node_gen is idempotent on an unknown node", ctx do
      assert :ok = PartitionMapWriter.delete_node_gen(ctx.writer, "never_seen")
    end
  end

  describe "drop_node/2" do
    test "drops every replica slot the node held", ctx do
      master = [{"test", 1, [{0, 0}]}]
      prole = [{"test", 1, [{0, 1}]}]

      assert PartitionMapWriter.apply_segments(ctx.writer, "A1", master, ["test"])
      assert PartitionMapWriter.apply_segments(ctx.writer, "B1", prole, ["test"])

      # Both nodes own part of partition 0 at master and prole respectively.
      {:ok, before} = PartitionMap.owners(ctx.tables.owners, "test", 0)
      assert before.replicas == ["A1", "B1"]

      assert :ok = PartitionMapWriter.drop_node(ctx.writer, "A1")

      {:ok, after_drop} = PartitionMap.owners(ctx.tables.owners, "test", 0)
      # `drop_node/2` compacts the replica list — the only remaining
      # replica is `B1`.
      assert after_drop.replicas == ["B1"]
    end

    test "deletes the owners row outright when the node was the last replica", ctx do
      segments = [{"test", 1, [{0, 0}]}]

      assert PartitionMapWriter.apply_segments(ctx.writer, "A1", segments, ["test"])
      assert :ok = PartitionMapWriter.drop_node(ctx.writer, "A1")

      assert {:error, :unknown_partition} = PartitionMap.owners(ctx.tables.owners, "test", 0)
    end
  end

  describe "recompute_ready/2" do
    test "returns false when any partition is unowned", ctx do
      # Only partition 0 is covered — the map is incomplete.
      segments = [{"test", 1, [{0, 0}]}]
      assert PartitionMapWriter.apply_segments(ctx.writer, "A1", segments, ["test"])

      refute PartitionMapWriter.recompute_ready(ctx.writer, ["test"])
      assert :ets.lookup(ctx.tables.meta, :ready) == [{:ready, false}]
    end

    test "returns true and flips the flag when every partition is covered", ctx do
      ownership = for pid <- 0..4095, do: {pid, 0}
      segments = [{"test", 1, ownership}]

      assert PartitionMapWriter.apply_segments(ctx.writer, "A1", segments, ["test"])

      assert PartitionMapWriter.recompute_ready(ctx.writer, ["test"])
      assert :ets.lookup(ctx.tables.meta, :ready) == [{:ready, true}]
    end

    test "collapses to false if any namespace is incomplete", ctx do
      full = for pid <- 0..4095, do: {pid, 0}
      segments = [{"test", 1, full}, {"other", 1, [{0, 0}]}]

      assert PartitionMapWriter.apply_segments(ctx.writer, "A1", segments, ["test", "other"])

      refute PartitionMapWriter.recompute_ready(ctx.writer, ["test", "other"])
    end
  end

  describe "published meta rows" do
    test "publish_retry_policy/2 stores the policy in the meta table", ctx do
      policy = %{max_retries: 4, sleep_between_retries_ms: 25, replica_policy: :master}

      assert :ok = PartitionMapWriter.publish_retry_policy(ctx.writer, policy)
      assert RetryPolicy.load(ctx.tables.meta) == policy
    end

    test "publish_active_nodes/2 stores a sorted unique node snapshot", ctx do
      assert :ok = PartitionMapWriter.publish_active_nodes(ctx.writer, ["B1", "A1", "A1"])

      assert :ets.lookup(ctx.tables.meta, :active_nodes) == [{:active_nodes, ["A1", "B1"]}]
    end
  end

  describe "crash semantics under a supervisor" do
    test "killing the writer restarts it without crashing the owner" do
      name = :"writer_crash_ok_#{:erlang.phash2(self())}"

      {:ok, owner} = TableOwner.start_link(name: name)
      tables = TableOwner.tables(owner)

      {:ok, sup} =
        Supervisor.start_link(
          [
            %{
              id: {PartitionMapWriter, name},
              start: {PartitionMapWriter, :start_link, [[name: name, tables: tables]]},
              type: :worker,
              restart: :permanent,
              shutdown: 5_000
            }
          ],
          strategy: :one_for_one
        )

      on_exit(fn ->
        stop_if_alive(sup)
        stop_if_alive(owner)
      end)

      before_pid = Process.whereis(PartitionMapWriter.via(name))
      assert is_pid(before_pid)

      Process.exit(before_pid, :kill)
      after_pid = await_replaced(PartitionMapWriter.via(name), before_pid)

      assert after_pid != before_pid
      assert Process.alive?(owner)

      # The restarted writer still drives the same tables — `TableOwner`
      # outlived the crash, so writes through the new pid land in the
      # pre-existing ETS tables.
      assert :ok = PartitionMapWriter.put_node_gen(after_pid, "A1", 9)
      assert {:ok, 9} = PartitionMap.get_node_gen(tables.node_gens, "A1")
    end
  end

  ## Helpers

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
