defmodule Aerospike.AdminTest do
  use ExUnit.Case, async: false

  alias Aerospike.Admin
  alias Aerospike.Policy
  alias Aerospike.Tables

  defp start_ets(name) do
    :ets.new(Tables.nodes(name), [:set, :public, :named_table, read_concurrency: true])
    :ets.new(Tables.meta(name), [:set, :public, :named_table])

    on_exit(fn ->
      for t <- [Tables.nodes(name), Tables.meta(name)] do
        try do
          :ets.delete(t)
        catch
          :error, :badarg -> :ok
        end
      end
    end)
  end

  describe "nodes/1" do
    setup do
      name = :"admin_utest_#{:erlang.unique_integer([:positive])}"
      start_ets(name)
      {:ok, name: name}
    end

    test "returns cluster_not_ready when cluster has not finished tending", %{name: name} do
      assert {:error, %{code: :cluster_not_ready}} = Admin.nodes(name)
    end

    test "returns empty list when no nodes are registered", %{name: name} do
      :ets.insert(Tables.meta(name), {Tables.ready_key(), true})
      assert {:ok, []} = Admin.nodes(name)
    end

    test "returns node entries from ETS", %{name: name} do
      :ets.insert(Tables.meta(name), {Tables.ready_key(), true})

      :ets.insert(Tables.nodes(name), {"node1", %{host: "127.0.0.1", port: 3000, active: true}})
      :ets.insert(Tables.nodes(name), {"node2", %{host: "127.0.0.2", port: 3001, active: true}})

      assert {:ok, nodes} = Admin.nodes(name)
      names = Enum.map(nodes, & &1.name) |> Enum.sort()
      assert names == ["node1", "node2"]

      node1 = Enum.find(nodes, &(&1.name == "node1"))
      assert node1.host == "127.0.0.1"
      assert node1.port == 3000
    end
  end

  describe "node_names/1" do
    setup do
      name = :"admin_utest_#{:erlang.unique_integer([:positive])}"
      start_ets(name)
      {:ok, name: name}
    end

    test "returns cluster_not_ready before cluster is ready", %{name: name} do
      assert {:error, %{code: :cluster_not_ready}} = Admin.node_names(name)
    end

    test "returns empty list when no nodes are registered", %{name: name} do
      :ets.insert(Tables.meta(name), {Tables.ready_key(), true})
      assert {:ok, []} = Admin.node_names(name)
    end

    test "returns sorted list of node names", %{name: name} do
      :ets.insert(Tables.meta(name), {Tables.ready_key(), true})
      :ets.insert(Tables.nodes(name), {"node-b", %{host: "h", port: 1, active: true}})
      :ets.insert(Tables.nodes(name), {"node-a", %{host: "h", port: 2, active: true}})

      assert {:ok, names} = Admin.node_names(name)
      assert Enum.sort(names) == ["node-a", "node-b"]
    end
  end

  describe "Policy.validate_info/1" do
    test "accepts empty opts" do
      assert {:ok, []} = Policy.validate_info([])
    end

    test "accepts timeout and pool_checkout_timeout" do
      assert {:ok, opts} = Policy.validate_info(timeout: 1_000, pool_checkout_timeout: 2_000)
      assert opts[:timeout] == 1_000
      assert opts[:pool_checkout_timeout] == 2_000
    end

    test "rejects non-integer timeout" do
      assert {:error, %NimbleOptions.ValidationError{}} = Policy.validate_info(timeout: "bad")
    end

    test "rejects unknown keys" do
      assert {:error, %NimbleOptions.ValidationError{}} = Policy.validate_info(unknown: true)
    end
  end

  describe "Aerospike facade info validation" do
    test "rejects bad opts and returns parameter_error" do
      assert {:error, %Aerospike.Error{code: :parameter_error}} =
               Aerospike.info(:not_a_real_conn, "namespaces", timeout: -1)
    end

    test "rejects unknown opts" do
      assert {:error, %Aerospike.Error{code: :parameter_error}} =
               Aerospike.info(:not_a_real_conn, "namespaces", unknown_opt: true)
    end
  end
end
