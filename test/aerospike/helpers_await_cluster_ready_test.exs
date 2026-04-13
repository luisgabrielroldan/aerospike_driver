defmodule Aerospike.Test.HelpersAwaitClusterReadyTest do
  use ExUnit.Case, async: false

  alias Aerospike.TableOwner
  alias Aerospike.Tables
  alias Aerospike.Test.Helpers

  setup do
    name = :"await_ready_test_#{:erlang.unique_integer([:positive])}"
    _pid = start_supervised!({TableOwner, name: name})
    {:ok, name: name}
  end

  test "returns :ok when ready_key is true and partition map is complete", %{name: name} do
    seed_ready(name, "test", 4096)

    assert :ok = Helpers.await_cluster_ready(name, timeout: 200, interval: 10)
  end

  test "honors a custom :namespace option", %{name: name} do
    seed_ready(name, "other", 4096)

    assert :ok =
             Helpers.await_cluster_ready(name,
               namespace: "other",
               timeout: 200,
               interval: 10
             )
  end

  test "honors :partitions_per_namespace for smaller fixtures", %{name: name} do
    seed_ready(name, "test", 16)

    assert :ok =
             Helpers.await_cluster_ready(name,
               timeout: 200,
               interval: 10,
               partitions_per_namespace: 16
             )
  end

  test "flunks on timeout when ready_key is set but partitions are missing", %{name: name} do
    :ets.insert(Tables.meta(name), {Tables.ready_key(), true})

    assert_raise ExUnit.AssertionError, ~r/poll_until\/2 timed out/, fn ->
      Helpers.await_cluster_ready(name, timeout: 50, interval: 10)
    end
  end

  test "flunks on timeout when neither flag nor partitions are set", %{name: name} do
    assert_raise ExUnit.AssertionError, ~r/poll_until\/2 timed out/, fn ->
      Helpers.await_cluster_ready(name, timeout: 50, interval: 10)
    end
  end

  test "between callback no-ops when no cluster GenServer is registered", %{name: name} do
    # No `*_cluster` process exists for this `name`, so `poke_tend` must
    # not raise when invoked from the between callback.
    refute Process.whereis(:"#{name}_cluster")

    assert_raise ExUnit.AssertionError, ~r/poll_until\/2 timed out/, fn ->
      Helpers.await_cluster_ready(name, timeout: 50, interval: 10)
    end
  end

  defp seed_ready(name, namespace, partitions) do
    :ets.insert(Tables.meta(name), {Tables.ready_key(), true})

    rows =
      for partition_id <- 0..(partitions - 1) do
        {{namespace, partition_id, 0}, :node_a}
      end

    :ets.insert(Tables.partitions(name), rows)
  end
end
