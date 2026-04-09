defmodule Aerospike.Repo do
  @moduledoc """
  Optional Repo-style convenience wrapper for `Aerospike`.

  `use Aerospike.Repo` generates a module that binds one connection name and
  exposes the same operations as `Aerospike`, but without the leading `conn`
  argument.

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

  Repo is optional sugar. `Aerospike` remains the canonical API.
  """

  @type t :: module()

  @conn_delegates [
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
    {:batch_exists, [:keys, {:opts, []}]},
    {:batch_exists!, [:keys, {:opts, []}]},
    {:batch_operate, [:ops, {:opts, []}]},
    {:batch_operate!, [:ops, {:opts, []}]},
    {:stream!, [:scannable, {:opts, []}]},
    {:all, [:scannable, {:opts, []}]},
    {:all!, [:scannable, {:opts, []}]},
    {:count, [:scannable, {:opts, []}]},
    {:count!, [:scannable, {:opts, []}]},
    {:page, [:scannable, {:opts, []}]},
    {:page!, [:scannable, {:opts, []}]},
    {:info, [:command, {:opts, []}]},
    {:info_node, [:node_name, :command, {:opts, []}]},
    {:nodes, []},
    {:node_names, []},
    {:create_index, [:namespace, :set, :opts]},
    {:drop_index, [:namespace, :index_name]},
    {:commit, [:txn]},
    {:abort, [:txn]},
    {:txn_status, [:txn]}
  ]

  @doc false
  defmacro __using__(opts) do
    otp_app = Keyword.fetch!(opts, :otp_app)
    repo_name = Keyword.get(opts, :name)
    adapter = Keyword.get(opts, :adapter, Aerospike)
    conn_wrappers = conn_wrapper_defs()

    conn_def =
      if repo_name do
        quote do
          def conn, do: unquote(repo_name)
        end
      else
        quote do
          def conn, do: __MODULE__
        end
      end

    quote do
      @repo_otp_app unquote(otp_app)
      @repo_name unquote(repo_name)
      @repo_adapter unquote(adapter)

      unquote(conn_def)

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
    end
  end

  defp conn_wrapper_defs do
    Enum.map(@conn_delegates, fn {fun_name, args} ->
      build_conn_wrapper(fun_name, args)
    end)
  end

  defp build_conn_wrapper(fun_name, args) do
    arg_defs = Enum.map(args, &arg_definition/1)
    arg_values = Enum.map(args, &arg_value/1)

    quote do
      def unquote(fun_name)(unquote_splicing(arg_defs)) do
        @repo_adapter.unquote(fun_name)(conn(), unquote_splicing(arg_values))
      end
    end
  end

  defp arg_definition({name, default}), do: {:\\, [], [{name, [], nil}, default]}
  defp arg_definition(name), do: {name, [], nil}

  defp arg_value({name, _default}), do: {name, [], nil}
  defp arg_value(name), do: {name, [], nil}
end
