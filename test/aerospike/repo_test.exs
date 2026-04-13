defmodule Aerospike.RepoTest do
  use ExUnit.Case, async: true

  defmodule FakeAdapter do
    def reset_calls!, do: Process.put({__MODULE__, :calls}, [])
    def calls, do: Enum.reverse(Process.get({__MODULE__, :calls}, []))

    defp record(fun, args, result) do
      current = Process.get({__MODULE__, :calls}, [])
      Process.put({__MODULE__, :calls}, [{fun, args} | current])
      result
    end

    def child_spec(opts),
      do:
        record(:child_spec, [opts], %{id: __MODULE__, start: {Task, :start_link, [fn -> :ok end]}})

    def start_link(opts), do: record(:start_link, [opts], {:ok, self()})
    def close(conn, timeout), do: record(:close, [conn, timeout], :ok)

    def key(namespace, set, user_key),
      do: record(:key, [namespace, set, user_key], {:key, namespace, set, user_key})

    def key_digest(namespace, set, digest),
      do: record(:key_digest, [namespace, set, digest], {:digest_key, namespace, set, digest})

    def put(conn, key, bins, opts), do: record(:put, [conn, key, bins, opts], :ok)
    def get(conn, key, opts), do: record(:get, [conn, key, opts], {:ok, %{}})
    def delete(conn, key, opts), do: record(:delete, [conn, key, opts], {:ok, true})
    def batch_get(conn, keys, opts), do: record(:batch_get, [conn, keys, opts], {:ok, []})

    def batch_get_header(conn, keys, opts),
      do: record(:batch_get_header, [conn, keys, opts], {:ok, []})

    def batch_get_operate(conn, keys, ops, opts),
      do: record(:batch_get_operate, [conn, keys, ops, opts], {:ok, []})

    def batch_operate(conn, ops, opts), do: record(:batch_operate, [conn, ops, opts], {:ok, []})
    def batch_delete(conn, keys, opts), do: record(:batch_delete, [conn, keys, opts], {:ok, []})

    def batch_udf(conn, keys, package, function, args, opts),
      do: record(:batch_udf, [conn, keys, package, function, args, opts], {:ok, []})

    def query_stream(conn, query, opts),
      do: record(:query_stream, [conn, query, opts], {:ok, :q_stream})

    def query_stream!(conn, query, opts),
      do: record(:query_stream!, [conn, query, opts], :q_stream)

    def query_all(conn, query, opts), do: record(:query_all, [conn, query, opts], {:ok, []})
    def query_all!(conn, query, opts), do: record(:query_all!, [conn, query, opts], [])
    def query_count(conn, query, opts), do: record(:query_count, [conn, query, opts], {:ok, 0})
    def query_count!(conn, query, opts), do: record(:query_count!, [conn, query, opts], 0)

    def query_page(conn, query, opts),
      do: record(:query_page, [conn, query, opts], {:ok, :q_page})

    def query_page!(conn, query, opts), do: record(:query_page!, [conn, query, opts], :q_page)

    def stream!(conn, scannable, opts),
      do: record(:stream!, [conn, scannable, opts], :fake_stream)

    def all(conn, scannable, opts), do: record(:all, [conn, scannable, opts], {:ok, []})
    def page(conn, scannable, opts), do: record(:page, [conn, scannable, opts], {:ok, :fake_page})
    def info(conn, command, opts), do: record(:info, [conn, command, opts], {:ok, "ok"})
    def nodes(conn), do: record(:nodes, [conn], {:ok, []})
    def list_udfs(conn, opts \\ []), do: record(:list_udfs, [conn, opts], {:ok, []})
    def list_udfs!(conn, opts \\ []), do: record(:list_udfs!, [conn, opts], [])

    def query_execute(conn, query, ops, opts \\ []),
      do: record(:query_execute, [conn, query, ops, opts], {:ok, :execute_task})

    def query_execute!(conn, query, ops, opts \\ []),
      do: record(:query_execute!, [conn, query, ops, opts], :execute_task)

    def query_execute_node(conn, node_name, query, ops, opts \\ []),
      do: record(:query_execute_node, [conn, node_name, query, ops, opts], {:ok, :execute_task})

    def query_execute_node!(conn, node_name, query, ops, opts \\ []),
      do: record(:query_execute_node!, [conn, node_name, query, ops, opts], :execute_task)

    def query_udf(conn, query, package, function, args, opts \\ []),
      do: record(:query_udf, [conn, query, package, function, args, opts], {:ok, :execute_task})

    def query_udf!(conn, query, package, function, args, opts \\ []),
      do: record(:query_udf!, [conn, query, package, function, args, opts], :execute_task)

    def query_udf_node(conn, node_name, query, package, function, args, opts \\ []),
      do:
        record(
          :query_udf_node,
          [conn, node_name, query, package, function, args, opts],
          {:ok, :execute_task}
        )

    def query_udf_node!(conn, node_name, query, package, function, args, opts \\ []),
      do:
        record(
          :query_udf_node!,
          [conn, node_name, query, package, function, args, opts],
          :execute_task
        )

    def query_aggregate(conn, query, package, function, args, opts \\ []),
      do:
        record(:query_aggregate, [conn, query, package, function, args, opts], {:ok, :agg_stream})

    def query_aggregate!(conn, query, package, function, args, opts \\ []),
      do: record(:query_aggregate!, [conn, query, package, function, args, opts], :agg_stream)

    def transaction(conn, fun), do: record(:transaction_2, [conn, fun], {:ok, :tx_ok})

    def transaction(conn, txn_or_opts, fun),
      do: record(:transaction_3, [conn, txn_or_opts, fun], {:ok, :tx_ok})

    def commit(conn, txn), do: record(:commit, [conn, txn], {:ok, :committed})
    def abort(conn, txn), do: record(:abort, [conn, txn], {:ok, :aborted})

    def create_user(conn, user_name, password, roles, opts \\ []),
      do: record(:create_user, [conn, user_name, password, roles, opts], :ok)

    def create_pki_user(conn, user_name, roles, opts \\ []),
      do: record(:create_pki_user, [conn, user_name, roles, opts], :ok)

    def drop_user(conn, user_name, opts \\ []),
      do: record(:drop_user, [conn, user_name, opts], :ok)

    def change_password(conn, user_name, password, opts \\ []),
      do: record(:change_password, [conn, user_name, password, opts], :ok)

    def grant_roles(conn, user_name, roles, opts \\ []),
      do: record(:grant_roles, [conn, user_name, roles, opts], :ok)

    def revoke_roles(conn, user_name, roles, opts \\ []),
      do: record(:revoke_roles, [conn, user_name, roles, opts], :ok)

    def query_user(conn, user_name, opts \\ []),
      do: record(:query_user, [conn, user_name, opts], {:ok, :user})

    def query_users(conn, opts \\ []),
      do: record(:query_users, [conn, opts], {:ok, []})

    def create_role(conn, role_name, privileges, opts \\ []),
      do: record(:create_role, [conn, role_name, privileges, opts], :ok)

    def drop_role(conn, role_name, opts \\ []),
      do: record(:drop_role, [conn, role_name, opts], :ok)

    def grant_privileges(conn, role_name, privileges, opts \\ []),
      do: record(:grant_privileges, [conn, role_name, privileges, opts], :ok)

    def revoke_privileges(conn, role_name, privileges, opts \\ []),
      do: record(:revoke_privileges, [conn, role_name, privileges, opts], :ok)

    def set_whitelist(conn, role_name, whitelist, opts \\ []),
      do: record(:set_whitelist, [conn, role_name, whitelist, opts], :ok)

    def set_quotas(conn, role_name, read_quota, write_quota, opts \\ []),
      do: record(:set_quotas, [conn, role_name, read_quota, write_quota, opts], :ok)

    def query_role(conn, role_name, opts \\ []),
      do: record(:query_role, [conn, role_name, opts], {:ok, :role})

    def query_roles(conn, opts \\ []),
      do: record(:query_roles, [conn, opts], {:ok, []})
  end

  defmodule DefaultRepo do
    @compile {:no_warn_undefined, Aerospike.RepoTest.FakeAdapter}
    use Aerospike.Repo, otp_app: :aerospike_driver, adapter: Aerospike.RepoTest.FakeAdapter
  end

  defmodule NamedRepo do
    @compile {:no_warn_undefined, Aerospike.RepoTest.FakeAdapter}
    use Aerospike.Repo,
      otp_app: :aerospike_driver,
      name: :repo_conn,
      adapter: Aerospike.RepoTest.FakeAdapter
  end

  @meta_functions [{:module_info, 0}, {:module_info, 1}, {:__info__, 1}]
  @repo_only_functions [{:conn, 0}, {:config, 0}]
  @admin_only_functions [{:conn, 0}]
  @non_conn_passthrough [:key, :key_digest]
  @repo_intentional_extras [{:child_spec, 1}, {:start_link, 1}]

  setup do
    Application.delete_env(:aerospike_driver, DefaultRepo)
    Application.delete_env(:aerospike_driver, NamedRepo)
    FakeAdapter.reset_calls!()
    :ok
  end

  describe "config/0 and conn/0" do
    test "uses module name as default connection when name is not set" do
      Application.put_env(:aerospike_driver, DefaultRepo, hosts: ["127.0.0.1:3000"])

      assert DefaultRepo.conn() == DefaultRepo
      assert Keyword.fetch!(DefaultRepo.config(), :name) == DefaultRepo
      assert Keyword.fetch!(DefaultRepo.config(), :hosts) == ["127.0.0.1:3000"]
    end

    test "uses explicit configured repo name from use options" do
      Application.put_env(:aerospike_driver, NamedRepo, hosts: ["127.0.0.1:3000"])

      assert NamedRepo.conn() == :repo_conn
      assert Keyword.fetch!(NamedRepo.config(), :name) == :repo_conn
    end
  end

  describe "startup wrappers" do
    test "child_spec/1 merges config and forwards options" do
      Application.put_env(:aerospike_driver, NamedRepo, hosts: ["127.0.0.1:3000"], pool_size: 8)
      _spec = NamedRepo.child_spec(pool_size: 12)

      assert [{:child_spec, [opts]}] = FakeAdapter.calls()
      assert Keyword.fetch!(opts, :hosts) == ["127.0.0.1:3000"]
      assert Keyword.fetch!(opts, :pool_size) == 12
      assert Keyword.fetch!(opts, :name) == :repo_conn
    end

    test "start_link/1 merges config and forwards options" do
      Application.put_env(:aerospike_driver, NamedRepo, hosts: ["127.0.0.1:3000"])
      assert {:ok, _pid} = NamedRepo.start_link(connect_timeout: 9_000)

      assert [{:start_link, [opts]}] = FakeAdapter.calls()
      assert Keyword.fetch!(opts, :hosts) == ["127.0.0.1:3000"]
      assert Keyword.fetch!(opts, :connect_timeout) == 9_000
      assert Keyword.fetch!(opts, :name) == :repo_conn
    end
  end

  describe "wrapper delegation" do
    test "delegates representative functions with bound conn" do
      key = {"test", "users", "u1"}
      scannable = :scan_query
      txn = :txn_handle

      assert :ok = NamedRepo.put(key, %{"n" => 1}, timeout: 100)
      assert {:ok, %{}} = NamedRepo.get(key, bins: ["n"])
      assert {:ok, true} = NamedRepo.delete(key, [])
      assert {:ok, []} = NamedRepo.batch_get([key], timeout: 1_000)
      assert {:ok, []} = NamedRepo.batch_get_header([key], timeout: 1_000)
      assert {:ok, []} = NamedRepo.batch_get_operate([key], [:read_op], timeout: 1_000)
      assert {:ok, []} = NamedRepo.batch_operate([], timeout: 1_000)
      assert {:ok, []} = NamedRepo.batch_delete([key], timeout: 1_000)
      assert {:ok, []} = NamedRepo.batch_udf([key], "pkg", "fn", [], timeout: 1_000)
      assert {:ok, :q_stream} = NamedRepo.query_stream(scannable, timeout: 2_000)
      assert :q_stream = NamedRepo.query_stream!(scannable, timeout: 2_000)
      assert {:ok, []} = NamedRepo.query_all(scannable, timeout: 2_000)
      assert [] = NamedRepo.query_all!(scannable, timeout: 2_000)
      assert {:ok, 0} = NamedRepo.query_count(scannable, timeout: 2_000)
      assert 0 = NamedRepo.query_count!(scannable, timeout: 2_000)
      assert {:ok, :q_page} = NamedRepo.query_page(scannable, timeout: 2_000)
      assert :q_page = NamedRepo.query_page!(scannable, timeout: 2_000)
      assert :fake_stream = NamedRepo.stream!(scannable, timeout: 2_000)
      assert {:ok, []} = NamedRepo.all(scannable, timeout: 2_000)
      assert {:ok, :fake_page} = NamedRepo.page(scannable, timeout: 2_000)
      assert {:ok, "ok"} = NamedRepo.info("namespaces", timeout: 2_000)
      assert {:ok, []} = NamedRepo.nodes()
      assert {:ok, []} = NamedRepo.list_udfs(timeout: 2_000)
      assert [] = NamedRepo.list_udfs!(timeout: 2_000)
      assert {:ok, :execute_task} = NamedRepo.query_execute(scannable, [:op], timeout: 2_000)
      assert :execute_task = NamedRepo.query_execute!(scannable, [:op], timeout: 2_000)

      assert {:ok, :execute_task} =
               NamedRepo.query_execute_node("node-a", scannable, [:op], timeout: 2_000)

      assert :execute_task =
               NamedRepo.query_execute_node!("node-a", scannable, [:op], timeout: 2_000)

      assert {:ok, :execute_task} =
               NamedRepo.query_udf(scannable, "pkg", "fn", [], timeout: 2_000)

      assert :execute_task = NamedRepo.query_udf!(scannable, "pkg", "fn", [], timeout: 2_000)

      assert {:ok, :execute_task} =
               NamedRepo.query_udf_node("node-a", scannable, "pkg", "fn", [], timeout: 2_000)

      assert :execute_task =
               NamedRepo.query_udf_node!("node-a", scannable, "pkg", "fn", [], timeout: 2_000)

      assert {:ok, :agg_stream} =
               NamedRepo.query_aggregate(scannable, "pkg", "fn", [], timeout: 2_000)

      assert :agg_stream = NamedRepo.query_aggregate!(scannable, "pkg", "fn", [], timeout: 2_000)
      assert {:ok, :tx_ok} = NamedRepo.transaction(fn _ -> :ok end)
      assert {:ok, :tx_ok} = NamedRepo.transaction([timeout: 5_000], fn _ -> :ok end)
      assert {:ok, :committed} = NamedRepo.commit(txn)
      assert {:ok, :aborted} = NamedRepo.abort(txn)

      assert {:put, [:repo_conn, ^key, %{"n" => 1}, [timeout: 100]]} =
               Enum.at(FakeAdapter.calls(), 0)

      assert {:get, [:repo_conn, ^key, [bins: ["n"]]]} = Enum.at(FakeAdapter.calls(), 1)

      assert {:batch_get, [:repo_conn, [^key], [timeout: 1_000]]} =
               Enum.at(FakeAdapter.calls(), 3)

      assert {:batch_get_header, [:repo_conn, [^key], [timeout: 1_000]]} =
               Enum.at(FakeAdapter.calls(), 4)

      assert {:batch_get_operate, [:repo_conn, [^key], [:read_op], [timeout: 1_000]]} =
               Enum.at(FakeAdapter.calls(), 5)

      assert {:query_stream, [:repo_conn, ^scannable, [timeout: 2_000]]} =
               Enum.at(FakeAdapter.calls(), 9)

      assert {:stream!, [:repo_conn, ^scannable, [timeout: 2_000]]} =
               Enum.at(FakeAdapter.calls(), 17)

      assert {:query_execute_node, [:repo_conn, "node-a", scannable, [:op], [timeout: 2_000]]} in FakeAdapter.calls()

      assert {:query_udf_node,
              [:repo_conn, "node-a", scannable, "pkg", "fn", [], [timeout: 2_000]]} in FakeAdapter.calls()

      assert {:transaction_2, [:repo_conn, _fun]} = Enum.at(FakeAdapter.calls(), 34)
      assert {:commit, [:repo_conn, ^txn]} = Enum.at(FakeAdapter.calls(), 36)
      assert {:abort, [:repo_conn, ^txn]} = Enum.at(FakeAdapter.calls(), 37)
    end
  end

  describe "API parity guard" do
    test "repo exposes the same callable API surface as Aerospike (minus conn arg)" do
      expected =
        Aerospike.__info__(:functions)
        |> Enum.reject(&(&1 in @meta_functions))
        |> Enum.map(fn
          {name, arity} when name in @non_conn_passthrough -> {name, arity}
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

      missing = expected -- actual
      unexpected = actual -- expected

      assert missing == []
      assert unexpected == @repo_intentional_extras
    end
  end
end
