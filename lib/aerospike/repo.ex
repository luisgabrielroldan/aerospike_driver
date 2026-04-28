defmodule Aerospike.Repo do
  @moduledoc """
  Generates an application-owned facade for one supervised Aerospike cluster.

  `use Aerospike.Repo` binds a Repo module to one cluster name and exposes
  current `Aerospike` facade functions without requiring callers to pass that
  name on every operation.

  Define a Repo in your application:

      defmodule MyApp.Repo do
        use Aerospike.Repo, otp_app: :my_app
      end

  Then configure and supervise it:

      config :my_app, MyApp.Repo,
        transport: Aerospike.Transport.Tcp,
        hosts: ["127.0.0.1:3000"],
        namespaces: ["test"],
        pool_size: 2

      children = [
        MyApp.Repo
      ]

  The generated module delegates to the canonical `Aerospike` API with
  `conn/0` injected as the cluster name:

      key = MyApp.Repo.key("test", "users", "user:1")
      {:ok, _metadata} = MyApp.Repo.put(key, %{"name" => "Ada"})
      {:ok, record} = MyApp.Repo.get(key)

  A Repo is a thin module facade over one cluster. It does not perform schema
  mapping, changeset validation, query translation, or object reflection.
  """

  @typedoc "Application-owned Repo module generated with `use Aerospike.Repo`."
  @type t :: module()

  @typedoc "Option accepted by `use Aerospike.Repo`."
  @type option ::
          {:otp_app, atom()}
          | {:adapter, module()}
          | {:name, atom()}

  @typedoc """
  Options accepted by `use Aerospike.Repo`.

  `:otp_app` is required and names the application environment used by
  `config/0`. `:adapter` defaults to `Aerospike` and is mainly useful for
  tests. `:name` defaults to the generated Repo module and becomes the cluster
  identity injected into delegated calls.
  """
  @type opts :: [option()]

  @repo_delegates [
    {:get, [:key, {:bins, :all}, {:opts, []}]},
    {:get_header, [:key, {:opts, []}]},
    {:put, [:key, :bins, {:opts, []}]},
    {:put_payload, [:key, :payload, {:opts, []}]},
    {:put_payload!, [:key, :payload, {:opts, []}]},
    {:add, [:key, :bins, {:opts, []}]},
    {:append, [:key, :bins, {:opts, []}]},
    {:prepend, [:key, :bins, {:opts, []}]},
    {:exists, [:key, {:opts, []}]},
    {:touch, [:key, {:opts, []}]},
    {:delete, [:key, {:opts, []}]},
    {:operate, [:key, :operations, {:opts, []}]},
    {:apply_udf, [:key, :package, :function, :args, {:opts, []}]},
    {:batch_get, [:keys, {:bins, :all}, {:opts, []}]},
    {:batch_get_header, [:keys, {:opts, []}]},
    {:batch_exists, [:keys, {:opts, []}]},
    {:batch_get_operate, [:keys, :operations, {:opts, []}]},
    {:batch_delete, [:keys, {:opts, []}]},
    {:batch_udf, [:keys, :package, :function, :args, {:opts, []}]},
    {:batch_operate, [:entries, {:opts, []}]},
    {:scan_stream, [:scan, {:opts, []}]},
    {:scan_stream!, [:scan, {:opts, []}]},
    {:scan_all, [:scan, {:opts, []}]},
    {:scan_all!, [:scan, {:opts, []}]},
    {:scan_page, [:scan, {:opts, []}]},
    {:scan_page!, [:scan, {:opts, []}]},
    {:scan_count, [:scan, {:opts, []}]},
    {:scan_count!, [:scan, {:opts, []}]},
    {:query_stream, [:query, {:opts, []}]},
    {:query_stream!, [:query, {:opts, []}]},
    {:query_all, [:query, {:opts, []}]},
    {:query_all!, [:query, {:opts, []}]},
    {:query_count, [:query, {:opts, []}]},
    {:query_count!, [:query, {:opts, []}]},
    {:query_page, [:query, {:opts, []}]},
    {:query_page!, [:query, {:opts, []}]},
    {:query_aggregate, [:query, :package, :function, :args, {:opts, []}]},
    {:query_aggregate_result, [:query, :package, :function, :args, {:opts, []}]},
    {:query_aggregate_result!, [:query, :package, :function, :args, {:opts, []}]},
    {:query_execute, [:query, :operations, {:opts, []}]},
    {:query_udf, [:query, :package, :function, :args, {:opts, []}]},
    {:info, [:command, {:opts, []}]},
    {:info_node, [:node_name, :command, {:opts, []}]},
    {:nodes, []},
    {:node_names, []},
    {:metrics_enabled?, []},
    {:enable_metrics, [{:opts, []}]},
    {:disable_metrics, []},
    {:stats, []},
    {:warm_up, [{:opts, []}]},
    {:create_index, [:namespace, :set, {:opts, []}]},
    {:create_expression_index, [:namespace, :set, :expression, {:opts, []}]},
    {:drop_index, [:namespace, :index_name, {:opts, []}]},
    {:set_xdr_filter, [:datacenter, :namespace, :filter]},
    {:list_udfs, [{:opts, []}]},
    {:register_udf, [:path_or_content, :server_name, {:opts, []}]},
    {:remove_udf, [:server_name, {:opts, []}]},
    {:truncate, [:namespace, {:set_or_opts, []}]},
    {:truncate, [:namespace, :set, :opts]},
    {:transaction, [:fun]},
    {:transaction, [:txn_or_opts, :fun]},
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
    {:set_whitelist, [:role_name, :whitelist, {:opts, []}]},
    {:set_quotas, [:role_name, :read_quota, :write_quota, {:opts, []}]},
    {:grant_privileges, [:role_name, :privileges, {:opts, []}]},
    {:revoke_privileges, [:role_name, :privileges, {:opts, []}]},
    {:query_role, [:role_name, {:opts, []}]},
    {:query_roles, [{:opts, []}]}
  ]

  @doc false
  defmacro __using__(opts) do
    otp_app = Keyword.fetch!(opts, :otp_app)
    adapter = Keyword.get(opts, :adapter, Aerospike)
    conn = Keyword.get(opts, :name, __CALLER__.module)

    repo_wrappers = wrapper_defs(adapter, @repo_delegates)
    admin_wrappers = wrapper_defs(adapter, @admin_delegates)

    quote do
      @doc """
      Returns the cluster name injected into generated Repo calls.
      """
      @spec conn() :: atom()
      def conn, do: unquote(conn)

      @doc """
      Returns the runtime configuration for this Repo.

      Configuration is read from the `:otp_app` passed to `use Aerospike.Repo`
      under the generated Repo module key. Runtime options passed to
      `child_spec/1` or `start_link/1` are merged over these values. The
      configured cluster `:name` defaults to `conn/0`.
      """
      @spec config() :: keyword()
      def config do
        unquote(otp_app)
        |> Application.get_env(__MODULE__, [])
        |> Keyword.put_new(:name, conn())
      end

      @doc """
      Builds a child spec for supervising this Repo's Aerospike cluster.

      `opts` are cluster supervisor options merged over `config/0`.
      """
      @spec child_spec(keyword()) :: Supervisor.child_spec()
      def child_spec(opts) when is_list(opts) do
        unquote(adapter).child_spec(Keyword.merge(config(), opts))
      end

      @doc """
      Starts this Repo's Aerospike cluster.

      `opts` are cluster supervisor options merged over `config/0`.
      """
      @spec start_link(keyword()) :: Supervisor.on_start()
      def start_link(opts) when is_list(opts) do
        unquote(adapter).start_link(Keyword.merge(config(), opts))
      end

      @doc """
      Stops this Repo's Aerospike cluster using the adapter default timeout.
      """
      @spec close() :: :ok
      def close do
        unquote(adapter).close(conn())
      end

      @doc """
      Stops this Repo's Aerospike cluster with an explicit timeout in milliseconds.
      """
      @spec close(non_neg_integer()) :: :ok
      def close(timeout) when is_integer(timeout) and timeout >= 0 do
        unquote(adapter).close(conn(), timeout)
      end

      @doc """
      Builds an `Aerospike.Key` for this Repo.

      This is a convenience delegate to `Aerospike.key/3`; it does not attach
      the Repo name to the key.
      """
      @spec key(String.t(), String.t(), String.t() | integer()) :: Aerospike.Key.t()
      def key(namespace, set, user_key) do
        unquote(adapter).key(namespace, set, user_key)
      end

      @doc """
      Builds an `Aerospike.Key` from an existing 20-byte digest.
      """
      @spec key_digest(String.t(), String.t(), <<_::160>>) :: Aerospike.Key.t()
      def key_digest(namespace, set, digest) do
        unquote(adapter).key_digest(namespace, set, digest)
      end

      unquote_splicing(repo_wrappers)

      defmodule Admin do
        @moduledoc false

        def conn, do: unquote(conn)

        unquote_splicing(admin_wrappers)
      end
    end
  end

  defp wrapper_defs(adapter, delegates) do
    Enum.map(delegates, fn {fun_name, args} ->
      build_wrapper(adapter, fun_name, args)
    end)
  end

  defp build_wrapper(adapter, fun_name, args) do
    arg_defs = Enum.map(args, &arg_definition/1)
    arg_values = Enum.map(args, &arg_value/1)
    spec_args = Enum.map(args, fn _arg -> quote(do: term()) end)
    facade_arity = length(args) + 1

    doc =
      "Delegates to the configured adapter's `#{fun_name}/#{facade_arity}` with `conn/0` as the first argument."

    quote do
      @doc unquote(doc)
      @spec unquote(fun_name)(unquote_splicing(spec_args)) :: term()
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
