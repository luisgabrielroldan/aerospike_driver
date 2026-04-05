defmodule Aerospike.TableOwnerTest do
  use ExUnit.Case, async: false

  alias Aerospike.TableOwner
  alias Aerospike.Tables

  setup do
    name = :"to_test_#{:erlang.unique_integer([:positive])}"
    {:ok, pid} = TableOwner.start_link(name: name)

    on_exit(fn ->
      if Process.alive?(pid) do
        GenServer.stop(pid, :normal, 5_000)
      end
    end)

    {:ok, name: name}
  end

  test "creates named ETS tables", %{name: name} do
    assert :ets.info(Tables.nodes(name)) != :undefined
    assert :ets.info(Tables.partitions(name)) != :undefined
    assert :ets.info(Tables.txn_tracking(name)) != :undefined
    assert :ets.info(Tables.meta(name)) != :undefined
  end

  test "init raises when name value is not an atom" do
    assert_raise ArgumentError, ":name must be an atom", fn ->
      TableOwner.init(name: "not_atom")
    end
  end
end
