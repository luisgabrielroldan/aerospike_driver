defmodule Aerospike.RepoTest do
  use ExUnit.Case, async: true

  defmodule FakeAdapter do
    @adapter_functions [
      {:close, 1},
      {:close, 2},
      {:key, 3},
      {:key_digest, 3},
      {:get, 4},
      {:get_header, 3},
      {:put, 4},
      {:put_payload, 4},
      {:put_payload!, 4},
      {:add, 4},
      {:append, 4},
      {:prepend, 4},
      {:exists, 3},
      {:touch, 3},
      {:delete, 3},
      {:operate, 4},
      {:apply_udf, 6},
      {:batch_get, 4},
      {:batch_get_header, 3},
      {:batch_exists, 3},
      {:batch_get_operate, 4},
      {:batch_delete, 3},
      {:batch_udf, 6},
      {:batch_operate, 3},
      {:scan_stream, 3},
      {:scan_stream!, 3},
      {:scan_all, 3},
      {:scan_all!, 3},
      {:scan_page, 3},
      {:scan_page!, 3},
      {:scan_count, 3},
      {:scan_count!, 3},
      {:query_stream, 3},
      {:query_stream!, 3},
      {:query_all, 3},
      {:query_all!, 3},
      {:query_count, 3},
      {:query_count!, 3},
      {:query_page, 3},
      {:query_page!, 3},
      {:query_aggregate, 6},
      {:query_aggregate_result, 6},
      {:query_aggregate_result!, 6},
      {:query_execute, 4},
      {:query_udf, 6},
      {:info, 3},
      {:info_node, 4},
      {:nodes, 1},
      {:node_names, 1},
      {:metrics_enabled?, 1},
      {:enable_metrics, 2},
      {:disable_metrics, 1},
      {:stats, 1},
      {:warm_up, 2},
      {:create_index, 4},
      {:create_expression_index, 5},
      {:drop_index, 4},
      {:set_xdr_filter, 4},
      {:list_udfs, 2},
      {:register_udf, 4},
      {:remove_udf, 3},
      {:truncate, 3},
      {:truncate, 4},
      {:transaction, 2},
      {:transaction, 3},
      {:commit, 2},
      {:abort, 2},
      {:txn_status, 2},
      {:create_user, 5},
      {:create_pki_user, 4},
      {:drop_user, 3},
      {:change_password, 4},
      {:grant_roles, 4},
      {:revoke_roles, 4},
      {:query_user, 3},
      {:query_users, 2},
      {:create_role, 4},
      {:drop_role, 3},
      {:set_whitelist, 4},
      {:set_quotas, 5},
      {:grant_privileges, 4},
      {:revoke_privileges, 4},
      {:query_role, 3},
      {:query_roles, 2}
    ]

    def reset_calls!, do: Process.put({__MODULE__, :calls}, [])
    def calls, do: Enum.reverse(Process.get({__MODULE__, :calls}, []))

    def child_spec(opts) do
      record(:child_spec, [opts], %{id: __MODULE__, start: {Task, :start_link, [fn -> :ok end]}})
    end

    def start_link(opts), do: record(:start_link, [opts], {:ok, self()})

    for {fun_name, arity} <- @adapter_functions do
      args = Macro.generate_arguments(arity, __MODULE__)

      def unquote(fun_name)(unquote_splicing(args)) do
        record(unquote(fun_name), unquote(args))
      end
    end

    defp record(fun_name, args) do
      record(fun_name, args, {:adapter_return, fun_name, args})
    end

    defp record(fun_name, args, result) do
      Process.put({__MODULE__, :calls}, [{fun_name, args} | Process.get({__MODULE__, :calls}, [])])

      result
    end
  end

  defmodule DefaultRepo do
    use Aerospike.Repo, otp_app: :aerospike_driver, adapter: Aerospike.RepoTest.FakeAdapter
  end

  defmodule NamedRepo do
    use Aerospike.Repo,
      otp_app: :aerospike_driver,
      name: :repo_conn,
      adapter: Aerospike.RepoTest.FakeAdapter
  end

  @meta_functions [{:module_info, 0}, {:module_info, 1}, {:__info__, 1}]
  @repo_only_functions [{:conn, 0}, {:config, 0}]
  @admin_only_functions [{:conn, 0}]
  @non_conn_functions [:child_spec, :key, :key_digest, :start_link]
  @deprecated_alias_functions [
    {:stream!, 2},
    {:stream!, 3},
    {:all, 2},
    {:all, 3},
    {:all!, 2},
    {:all!, 3},
    {:count, 2},
    {:count, 3},
    {:count!, 2},
    {:count!, 3}
  ]

  setup do
    Application.delete_env(:aerospike_driver, DefaultRepo)
    Application.delete_env(:aerospike_driver, NamedRepo)
    FakeAdapter.reset_calls!()
    :ok
  end

  describe "configuration" do
    test "uses the repo module as the default connection name" do
      Application.put_env(:aerospike_driver, DefaultRepo, hosts: ["127.0.0.1:3000"])

      assert DefaultRepo.conn() == DefaultRepo
      assert Keyword.fetch!(DefaultRepo.config(), :name) == DefaultRepo
      assert Keyword.fetch!(DefaultRepo.config(), :hosts) == ["127.0.0.1:3000"]
    end

    test "uses the configured repo name from use options" do
      Application.put_env(:aerospike_driver, NamedRepo, hosts: ["127.0.0.1:3000"])

      assert NamedRepo.conn() == :repo_conn
      assert NamedRepo.Admin.conn() == :repo_conn
      assert Keyword.fetch!(NamedRepo.config(), :name) == :repo_conn
    end
  end

  describe "startup wrappers" do
    test "child_spec/1 merges config and lets caller options win" do
      Application.put_env(:aerospike_driver, NamedRepo,
        hosts: ["127.0.0.1:3000"],
        pool_size: 8
      )

      _spec = NamedRepo.child_spec(pool_size: 12)

      assert [{:child_spec, [opts]}] = FakeAdapter.calls()
      assert Keyword.fetch!(opts, :hosts) == ["127.0.0.1:3000"]
      assert Keyword.fetch!(opts, :pool_size) == 12
      assert Keyword.fetch!(opts, :name) == :repo_conn
    end

    test "start_link/1 injects the repo connection name when config omits it" do
      Application.put_env(:aerospike_driver, NamedRepo, hosts: ["127.0.0.1:3000"])

      assert {:ok, _pid} = NamedRepo.start_link(connect_timeout: 9_000)

      assert [{:start_link, [opts]}] = FakeAdapter.calls()
      assert Keyword.fetch!(opts, :hosts) == ["127.0.0.1:3000"]
      assert Keyword.fetch!(opts, :connect_timeout) == 9_000
      assert Keyword.fetch!(opts, :name) == :repo_conn
    end
  end

  describe "wrapper delegation" do
    test "delegates lifecycle and key helpers without altering return values" do
      assert {:adapter_return, :close, [:repo_conn]} = NamedRepo.close()
      assert {:adapter_return, :close, [:repo_conn, 1_000]} = NamedRepo.close(1_000)

      assert {:adapter_return, :key, ["test", "users", "u1"]} =
               NamedRepo.key("test", "users", "u1")

      assert {:adapter_return, :key_digest, ["test", "users", <<0::160>>]} =
               NamedRepo.key_digest("test", "users", <<0::160>>)
    end

    test "delegates representative data and operator functions with the bound connection" do
      key = {"test", "users", "u1"}
      query = :query
      scan = :scan
      txn = :txn

      assert {:adapter_return, :get, [:repo_conn, ^key, :all, []]} = NamedRepo.get(key)

      assert {:adapter_return, :get, [:repo_conn, ^key, ["name"], [timeout: 100]]} =
               NamedRepo.get(key, ["name"], timeout: 100)

      assert {:adapter_return, :batch_get, [:repo_conn, [^key], :all, [timeout: 100]]} =
               NamedRepo.batch_get([key], :all, timeout: 100)

      assert {:adapter_return, :scan_stream!, [:repo_conn, ^scan, [timeout: 100]]} =
               NamedRepo.scan_stream!(scan, timeout: 100)

      assert {:adapter_return, :query_aggregate_result!,
              [:repo_conn, ^query, "pkg", "fn", [], [source: "return 1"]]} =
               NamedRepo.query_aggregate_result!(query, "pkg", "fn", [], source: "return 1")

      assert {:adapter_return, :create_index,
              [:repo_conn, "test", "users", [name: "users_name", bin: "name", type: :string]]} =
               NamedRepo.create_index("test", "users",
                 name: "users_name",
                 bin: "name",
                 type: :string
               )

      assert {:adapter_return, :truncate, [:repo_conn, "test", []]} =
               NamedRepo.truncate("test")

      assert {:adapter_return, :truncate, [:repo_conn, "test", "users", []]} =
               NamedRepo.truncate("test", "users", [])

      assert {:adapter_return, :transaction, [:repo_conn, _fun]} =
               NamedRepo.transaction(fn _ -> :ok end)

      assert {:adapter_return, :transaction, [:repo_conn, [timeout: 100], _fun]} =
               NamedRepo.transaction([timeout: 100], fn _ -> :ok end)

      assert {:adapter_return, :commit, [:repo_conn, ^txn]} = NamedRepo.commit(txn)
      assert {:adapter_return, :abort, [:repo_conn, ^txn]} = NamedRepo.abort(txn)
      assert {:adapter_return, :txn_status, [:repo_conn, ^txn]} = NamedRepo.txn_status(txn)
    end

    test "delegates security administration through the generated Admin module" do
      assert {:adapter_return, :create_user, [:repo_conn, "ada", "secret", ["read-write"], []]} =
               NamedRepo.Admin.create_user("ada", "secret", ["read-write"])

      assert {:adapter_return, :set_quotas, [:repo_conn, "writers", 100, 50, [timeout: 100]]} =
               NamedRepo.Admin.set_quotas("writers", 100, 50, timeout: 100)

      assert {:adapter_return, :query_roles, [:repo_conn, []]} = NamedRepo.Admin.query_roles()
    end
  end

  describe "API surface guard" do
    test "generated modules expose the current Aerospike facade minus the bound cluster argument" do
      expected =
        Aerospike.__info__(:functions)
        |> Enum.reject(&(&1 in @meta_functions or &1 in @deprecated_alias_functions))
        |> Enum.map(fn
          {name, arity} when name in @non_conn_functions -> {name, arity}
          {name, arity} -> {name, arity - 1}
        end)
        |> Enum.sort()

      repo_surface =
        NamedRepo.__info__(:functions)
        |> Enum.reject(&(&1 in @meta_functions or &1 in @repo_only_functions))

      admin_surface =
        NamedRepo.Admin.__info__(:functions)
        |> Enum.reject(&(&1 in @meta_functions or &1 in @admin_only_functions))

      actual =
        (repo_surface ++ admin_surface)
        |> Enum.uniq()
        |> Enum.sort()

      assert actual == expected
    end
  end
end
