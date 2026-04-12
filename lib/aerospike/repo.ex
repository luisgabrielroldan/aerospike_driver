defmodule Aerospike.Repo do
  @moduledoc """
  Recommended application-facing wrapper for `Aerospike`.

  `use Aerospike.Repo` generates a module that binds one connection name and
  exposes the same operations as `Aerospike`, but without the leading `conn`
  argument.

  Security administration (users, roles, quotas, etc.) lives on the generated
  `YourApp.Repo.Admin` sub-module so the main Repo stays focused on data-plane
  calls. Do not define your own `YourApp.Repo.Admin` module in the same app;
  it would collide with the generated one.

  ## Example

      defmodule MyApp.Repo do
        use Aerospike.Repo,
          otp_app: :my_app,
          name: :aero
      end

      # config/runtime.exs
      config :my_app, MyApp.Repo,
        hosts: ["127.0.0.1:3000"],
        pool_size: 10

      # supervision
      children = [
        MyApp.Repo
      ]

      :ok = MyApp.Repo.put({"test", "users", "user:1"}, %{"name" => "Ada"})
      {:ok, record} = MyApp.Repo.get({"test", "users", "user:1"})

      # Security administration via the generated sub-module:
      :ok = MyApp.Repo.Admin.create_user("ada", "secret", ["read-write"])
      {:ok, roles} = MyApp.Repo.Admin.query_roles()

  Use this wrapper for normal application code. `Aerospike` remains the
  canonical low-level API when you need explicit connection control.
  """

  @type t :: module()

  @data_delegates [
    {:put, [:key, :bins, {:opts, []}]},
    {:put!, [:key, :bins, {:opts, []}]},
    {:get, [:key, {:opts, []}]},
    {:get!, [:key, {:opts, []}]},
    {:delete, [:key, {:opts, []}]},
    {:delete!, [:key, {:opts, []}]},
    {:exists, [:key, {:opts, []}]},
    {:exists!, [:key, {:opts, []}]},
    {:touch, [:key, {:opts, []}]},
    {:touch!, [:key, {:opts, []}]},
    {:operate, [:key, :ops, {:opts, []}]},
    {:operate!, [:key, :ops, {:opts, []}]},
    {:add, [:key, :bins, {:opts, []}]},
    {:add!, [:key, :bins, {:opts, []}]},
    {:append, [:key, :bins, {:opts, []}]},
    {:append!, [:key, :bins, {:opts, []}]},
    {:prepend, [:key, :bins, {:opts, []}]},
    {:prepend!, [:key, :bins, {:opts, []}]},
    {:batch_get, [:keys, {:opts, []}]},
    {:batch_get!, [:keys, {:opts, []}]},
    {:batch_get_header, [:keys, {:opts, []}]},
    {:batch_get_header!, [:keys, {:opts, []}]},
    {:batch_get_operate, [:keys, :ops, {:opts, []}]},
    {:batch_get_operate!, [:keys, :ops, {:opts, []}]},
    {:batch_exists, [:keys, {:opts, []}]},
    {:batch_exists!, [:keys, {:opts, []}]},
    {:batch_delete, [:keys, {:opts, []}]},
    {:batch_delete!, [:keys, {:opts, []}]},
    {:batch_udf, [:keys, :package, :function, :args, {:opts, []}]},
    {:batch_udf!, [:keys, :package, :function, :args, {:opts, []}]},
    {:batch_operate, [:ops, {:opts, []}]},
    {:batch_operate!, [:ops, {:opts, []}]},
    {:query_stream, [:query, {:opts, []}]},
    {:query_stream!, [:query, {:opts, []}]},
    {:query_stream_node, [:node_name, :query, {:opts, []}]},
    {:query_stream_node!, [:node_name, :query, {:opts, []}]},
    {:query_all, [:query, {:opts, []}]},
    {:query_all!, [:query, {:opts, []}]},
    {:query_all_node, [:node_name, :query, {:opts, []}]},
    {:query_all_node!, [:node_name, :query, {:opts, []}]},
    {:query_count, [:query, {:opts, []}]},
    {:query_count!, [:query, {:opts, []}]},
    {:query_count_node, [:node_name, :query, {:opts, []}]},
    {:query_count_node!, [:node_name, :query, {:opts, []}]},
    {:query_page, [:query, {:opts, []}]},
    {:query_page!, [:query, {:opts, []}]},
    {:query_page_node, [:node_name, :query, {:opts, []}]},
    {:query_page_node!, [:node_name, :query, {:opts, []}]},
    {:stream!, [:scannable, {:opts, []}]},
    {:scan_stream_node!, [:node_name, :scan, {:opts, []}]},
    {:all, [:scannable, {:opts, []}]},
    {:all!, [:scannable, {:opts, []}]},
    {:scan_all_node, [:node_name, :scan, {:opts, []}]},
    {:scan_all_node!, [:node_name, :scan, {:opts, []}]},
    {:count, [:scannable, {:opts, []}]},
    {:count!, [:scannable, {:opts, []}]},
    {:scan_count_node, [:node_name, :scan, {:opts, []}]},
    {:scan_count_node!, [:node_name, :scan, {:opts, []}]},
    {:page, [:scannable, {:opts, []}]},
    {:page!, [:scannable, {:opts, []}]},
    {:scan_page_node, [:node_name, :scan, {:opts, []}]},
    {:scan_page_node!, [:node_name, :scan, {:opts, []}]},
    {:info, [:command, {:opts, []}]},
    {:info_node, [:node_name, :command, {:opts, []}]},
    {:nodes, []},
    {:node_names, []},
    {:create_index, [:namespace, :set, :opts]},
    {:create_expression_index, [:namespace, :set, :expression, :opts]},
    {:drop_index, [:namespace, :index_name]},
    {:list_udfs, [{:opts, []}]},
    {:list_udfs!, [{:opts, []}]},
    {:query_execute, [:query, :ops, {:opts, []}]},
    {:query_execute!, [:query, :ops, {:opts, []}]},
    {:query_udf, [:query, :package, :function, :args, {:opts, []}]},
    {:query_udf!, [:query, :package, :function, :args, {:opts, []}]},
    {:query_aggregate, [:query, :package, :function, :args, {:opts, []}]},
    {:query_aggregate!, [:query, :package, :function, :args, {:opts, []}]},
    {:commit, [:txn]},
    {:abort, [:txn]},
    {:txn_status, [:txn]}
  ]

  @admin_delegates [
    {:create_user, [:user_name, :password, :roles, {:opts, []}]},
    {:create_pki_user, [:user_name, :roles, {:opts, []}]},
    {:drop_user, [:user_name, {:opts, []}]},
    {:change_password, [:user_name, :password, {:opts, []}]},
    {:grant_roles, [:user_name, :roles, {:opts, []}]},
    {:revoke_roles, [:user_name, :roles, {:opts, []}]},
    {:query_user, [:user_name, {:opts, []}]},
    {:query_users, [{:opts, []}]},
    {:create_role, [:role_name, :privileges, {:opts, []}]},
    {:drop_role, [:role_name, {:opts, []}]},
    {:grant_privileges, [:role_name, :privileges, {:opts, []}]},
    {:revoke_privileges, [:role_name, :privileges, {:opts, []}]},
    {:set_whitelist, [:role_name, :whitelist, {:opts, []}]},
    {:set_quotas, [:role_name, :read_quota, :write_quota, {:opts, []}]},
    {:query_role, [:role_name, {:opts, []}]},
    {:query_roles, [{:opts, []}]}
  ]

  @doc false
  defmacro __using__(opts) do
    otp_app = Keyword.fetch!(opts, :otp_app)
    repo_name = Keyword.get(opts, :name)
    adapter = Keyword.get(opts, :adapter, Aerospike)
    parent_module = __CALLER__.module

    admin_moduledoc =
      "Security administration operations using the same connection as " <>
        "`#{inspect(parent_module)}` (see `conn/0`)."

    conn_wrappers = conn_wrapper_defs(adapter)
    admin_wrappers = admin_wrapper_defs(adapter)

    conn_value =
      if repo_name do
        repo_name
      else
        quote(do: __MODULE__)
      end

    quote do
      @repo_otp_app unquote(otp_app)
      @repo_name unquote(repo_name)
      @repo_adapter unquote(adapter)

      def conn, do: unquote(conn_value)

      def config do
        @repo_otp_app
        |> Application.get_env(__MODULE__, [])
        |> Keyword.put_new(:name, conn())
      end

      def child_spec(opts \\ []) when is_list(opts) do
        @repo_adapter.child_spec(Keyword.merge(config(), opts))
      end

      def start_link(opts \\ []) when is_list(opts) do
        @repo_adapter.start_link(Keyword.merge(config(), opts))
      end

      def close(timeout \\ 15_000)
          when is_integer(timeout) and timeout >= 0 do
        @repo_adapter.close(conn(), timeout)
      end

      def key(namespace, set, user_key), do: @repo_adapter.key(namespace, set, user_key)

      def key_digest(namespace, set, digest), do: @repo_adapter.key_digest(namespace, set, digest)

      unquote_splicing(conn_wrappers)

      def truncate(namespace, set_or_opts \\ [])

      def truncate(namespace, opts) when is_binary(namespace) and is_list(opts) do
        @repo_adapter.truncate(conn(), namespace, opts)
      end

      def truncate(namespace, set) when is_binary(namespace) and is_binary(set) do
        @repo_adapter.truncate(conn(), namespace, set)
      end

      def truncate(namespace, set, opts)
          when is_binary(namespace) and is_binary(set) and is_list(opts) do
        @repo_adapter.truncate(conn(), namespace, set, opts)
      end

      def register_udf(path_or_content, server_name) do
        @repo_adapter.register_udf(conn(), path_or_content, server_name)
      end

      def register_udf(path_or_content, server_name, opts) when is_list(opts) do
        @repo_adapter.register_udf(conn(), path_or_content, server_name, opts)
      end

      def remove_udf(udf_name), do: @repo_adapter.remove_udf(conn(), udf_name)

      def remove_udf(udf_name, opts) when is_list(opts) do
        @repo_adapter.remove_udf(conn(), udf_name, opts)
      end

      def apply_udf(key, package, function, args) do
        @repo_adapter.apply_udf(conn(), key, package, function, args)
      end

      def apply_udf(key, package, function, args, opts) when is_list(opts) do
        @repo_adapter.apply_udf(conn(), key, package, function, args, opts)
      end

      def transaction(fun) when is_function(fun, 1) do
        @repo_adapter.transaction(conn(), fun)
      end

      def transaction(txn_or_opts, fun) when is_function(fun, 1) do
        @repo_adapter.transaction(conn(), txn_or_opts, fun)
      end

      defmodule Module.concat(__MODULE__, Admin) do
        @moduledoc unquote(admin_moduledoc)

        def conn, do: unquote(conn_value)

        unquote_splicing(admin_wrappers)
      end
    end
  end

  defp conn_wrapper_defs(adapter) do
    Enum.map(@data_delegates, fn {fun_name, args} ->
      build_conn_wrapper(adapter, fun_name, args)
    end)
  end

  defp admin_wrapper_defs(adapter) do
    Enum.map(@admin_delegates, fn {fun_name, args} ->
      build_conn_wrapper(adapter, fun_name, args)
    end)
  end

  defp build_conn_wrapper(adapter, fun_name, args) do
    arg_defs = Enum.map(args, &arg_definition/1)
    arg_values = Enum.map(args, &arg_value/1)

    quote do
      def unquote(fun_name)(unquote_splicing(arg_defs)) do
        unquote(adapter).unquote(fun_name)(conn(), unquote_splicing(arg_values))
      end
    end
  end

  defp arg_definition({name, default}), do: {:\\, [], [{name, [], nil}, default]}
  defp arg_definition(name), do: {name, [], nil}

  defp arg_value({name, _default}), do: {name, [], nil}
  defp arg_value(name), do: {name, [], nil}
end
