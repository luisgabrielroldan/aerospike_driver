defmodule Aerospike.Admin do
  @moduledoc false

  alias Aerospike.Error
  alias Aerospike.IndexTask
  alias Aerospike.RegisterTask
  alias Aerospike.Router
  alias Aerospike.Tables

  @default_checkout_timeout 5_000

  @doc false
  @spec info(atom(), String.t(), keyword()) :: {:ok, String.t()} | {:error, Error.t()}
  def info(conn_name, command, opts) when is_atom(conn_name) and is_binary(command) do
    checkout_timeout = Keyword.get(opts, :pool_checkout_timeout, @default_checkout_timeout)

    with_telemetry(:info, conn_name, fn ->
      with {:ok, pool_pid, node_name} <- Router.random_node_pool(conn_name),
           {:ok, map} <- Router.checkout_and_info(pool_pid, [command], checkout_timeout) do
        {{:ok, Map.get(map, command, "")}, node_name}
      else
        {:error, %Error{} = e} -> {{:error, e}, nil}
      end
    end)
  end

  @doc false
  @spec info_node(atom(), String.t(), String.t(), keyword()) ::
          {:ok, String.t()} | {:error, Error.t()}
  def info_node(conn_name, node_name, command, opts)
      when is_atom(conn_name) and is_binary(node_name) and is_binary(command) do
    checkout_timeout = Keyword.get(opts, :pool_checkout_timeout, @default_checkout_timeout)

    with_telemetry(:info_node, conn_name, fn ->
      with {:ok, pool_pid, ^node_name} <- Router.node_pool(conn_name, node_name),
           {:ok, map} <- Router.checkout_and_info(pool_pid, [command], checkout_timeout) do
        {{:ok, Map.get(map, command, "")}, node_name}
      else
        {:error, %Error{} = e} -> {{:error, e}, nil}
      end
    end)
  end

  @doc false
  @spec nodes(atom()) :: {:ok, [Aerospike.node_info()]} | {:error, Error.t()}
  def nodes(conn_name) when is_atom(conn_name) do
    with :ok <- check_ready(conn_name) do
      # `row` is `t:Aerospike.Cluster.node_row/0` from `insert_node_registry/5`.
      node_list =
        conn_name
        |> Tables.nodes()
        |> :ets.tab2list()
        |> Enum.map(fn {name, row} ->
          %{name: name, host: Map.get(row, :host, ""), port: Map.get(row, :port, 0)}
        end)

      {:ok, node_list}
    end
  end

  @doc false
  @spec node_names(atom()) :: {:ok, [String.t()]} | {:error, Error.t()}
  def node_names(conn_name) when is_atom(conn_name) do
    with :ok <- check_ready(conn_name) do
      names =
        conn_name
        |> Tables.nodes()
        |> :ets.tab2list()
        |> Enum.map(fn {name, _row} -> name end)

      {:ok, names}
    end
  end

  @doc false
  @spec truncate(atom(), String.t(), keyword()) :: :ok | {:error, Error.t()}
  def truncate(conn_name, namespace, opts) when is_atom(conn_name) and is_binary(namespace) do
    command = build_truncate_namespace_command(namespace, opts)
    checkout_timeout = Keyword.get(opts, :pool_checkout_timeout, @default_checkout_timeout)

    with_telemetry(:truncate, conn_name, fn ->
      with {:ok, pool_pid, node_name} <- Router.random_node_pool(conn_name),
           {:ok, map} <- Router.checkout_and_info(pool_pid, [command], checkout_timeout) do
        {parse_ok_response(map, command), node_name}
      else
        {:error, %Error{} = e} -> {{:error, e}, nil}
      end
    end)
  end

  @doc false
  @spec truncate(atom(), String.t(), String.t(), keyword()) :: :ok | {:error, Error.t()}
  def truncate(conn_name, namespace, set, opts)
      when is_atom(conn_name) and is_binary(namespace) and is_binary(set) do
    command = "truncate:namespace=#{namespace};set=#{set}"
    checkout_timeout = Keyword.get(opts, :pool_checkout_timeout, @default_checkout_timeout)

    with_telemetry(:truncate, conn_name, fn ->
      with {:ok, pool_pid, node_name} <- Router.random_node_pool(conn_name),
           {:ok, map} <- Router.checkout_and_info(pool_pid, [command], checkout_timeout) do
        {parse_ok_response(map, command), node_name}
      else
        {:error, %Error{} = e} -> {{:error, e}, nil}
      end
    end)
  end

  @doc false
  @spec create_index(atom(), String.t(), String.t(), keyword()) ::
          {:ok, IndexTask.t()} | {:error, Error.t()}
  def create_index(conn_name, namespace, set, opts)
      when is_atom(conn_name) and is_binary(namespace) and is_binary(set) and is_list(opts) do
    bin = Keyword.fetch!(opts, :bin)
    name = Keyword.fetch!(opts, :name)
    type = Keyword.fetch!(opts, :type)
    collection = Keyword.get(opts, :collection)
    checkout_timeout = Keyword.get(opts, :pool_checkout_timeout, @default_checkout_timeout)

    command = build_create_index_command(namespace, set, name, bin, type, collection)

    with_telemetry(:create_index, conn_name, fn ->
      with {:ok, pool_pid, node_name} <- Router.random_node_pool(conn_name),
           {:ok, map} <- Router.checkout_and_info(pool_pid, [command], checkout_timeout) do
        result = parse_create_index_response(map, command, conn_name, namespace, name)
        {result, node_name}
      else
        {:error, %Error{} = e} -> {{:error, e}, nil}
      end
    end)
  end

  @doc false
  @spec drop_index(atom(), String.t(), String.t(), keyword()) :: :ok | {:error, Error.t()}
  def drop_index(conn_name, namespace, index_name, opts)
      when is_atom(conn_name) and is_binary(namespace) and is_binary(index_name) and
             is_list(opts) do
    command = "sindex-delete:namespace=#{namespace};indexname=#{index_name}"
    checkout_timeout = Keyword.get(opts, :pool_checkout_timeout, @default_checkout_timeout)

    with_telemetry(:drop_index, conn_name, fn ->
      with {:ok, pool_pid, node_name} <- Router.random_node_pool(conn_name),
           {:ok, map} <- Router.checkout_and_info(pool_pid, [command], checkout_timeout) do
        {parse_drop_index_response(map, command), node_name}
      else
        {:error, %Error{} = e} -> {{:error, e}, nil}
      end
    end)
  end

  @doc false
  @spec register_udf(atom(), String.t(), String.t(), keyword()) ::
          {:ok, RegisterTask.t()} | {:error, Error.t()}
  def register_udf(conn_name, path_or_content, server_name, opts)
      when is_atom(conn_name) and is_binary(path_or_content) and is_binary(server_name) and
             is_list(opts) do
    case read_udf_content(path_or_content) do
      {:error, _} = err -> err
      {:ok, content} -> do_register_udf(conn_name, content, server_name, opts)
    end
  end

  defp do_register_udf(conn_name, content, server_name, opts) do
    encoded = Base.encode64(content)
    content_len = byte_size(encoded)

    command =
      "udf-put:filename=#{server_name};content=#{encoded};content-len=#{content_len};udf-type=LUA;"

    checkout_timeout = Keyword.get(opts, :pool_checkout_timeout, @default_checkout_timeout)

    with_telemetry(:register_udf, conn_name, fn ->
      with {:ok, pool_pid, node_name} <- Router.random_node_pool(conn_name),
           {:ok, map} <- Router.checkout_and_info(pool_pid, [command], checkout_timeout) do
        result = parse_register_udf_response(map, command, conn_name, server_name)
        {result, node_name}
      else
        {:error, %Error{} = e} -> {{:error, e}, nil}
      end
    end)
  end

  @doc false
  @spec remove_udf(atom(), String.t(), keyword()) :: :ok | {:error, Error.t()}
  def remove_udf(conn_name, udf_name, opts)
      when is_atom(conn_name) and is_binary(udf_name) and is_list(opts) do
    command = "udf-remove:filename=#{udf_name};"
    checkout_timeout = Keyword.get(opts, :pool_checkout_timeout, @default_checkout_timeout)

    with_telemetry(:remove_udf, conn_name, fn ->
      with {:ok, pool_pid, node_name} <- Router.random_node_pool(conn_name),
           {:ok, map} <- Router.checkout_and_info(pool_pid, [command], checkout_timeout) do
        {parse_ok_response(map, command), node_name}
      else
        {:error, %Error{} = e} -> {{:error, e}, nil}
      end
    end)
  end

  defp build_create_index_command(namespace, set, name, bin, type, collection) do
    base = "sindex-create:namespace=#{namespace};set=#{set};indexname=#{name}"

    base =
      case collection do
        nil -> base
        coll -> base <> ";indextype=#{collection_type_string(coll)}"
      end

    base <> ";bin=#{bin};type=#{index_type_string(type)}"
  end

  defp index_type_string(:numeric), do: "NUMERIC"
  defp index_type_string(:string), do: "STRING"
  defp index_type_string(:geo2dsphere), do: "GEO2DSPHERE"

  defp collection_type_string(:list), do: "LIST"
  defp collection_type_string(:mapkeys), do: "MAPKEYS"
  defp collection_type_string(:mapvalues), do: "MAPVALUES"

  defp parse_create_index_response(map, command, conn_name, namespace, index_name) do
    response = Map.get(map, command, "")

    if String.downcase(String.trim(response)) == "ok" do
      {:ok, %IndexTask{conn: conn_name, namespace: namespace, index_name: index_name}}
    else
      {:error,
       Error.from_result_code(:server_error,
         message: "unexpected info response: #{inspect(response)}"
       )}
    end
  end

  defp parse_drop_index_response(map, command) do
    response = Map.get(map, command, "")
    lower = String.downcase(String.trim(response))

    cond do
      lower == "ok" ->
        :ok

      String.contains?(lower, "not found") or String.contains?(lower, "notfound") ->
        :ok

      true ->
        {:error,
         Error.from_result_code(:server_error,
           message: "unexpected info response: #{inspect(response)}"
         )}
    end
  end

  defp build_truncate_namespace_command(namespace, opts) do
    base = "truncate-namespace:namespace=#{namespace}"

    case Keyword.get(opts, :before) do
      nil -> base
      %DateTime{} = dt -> base <> ";lut=#{DateTime.to_unix(dt, :nanosecond)}"
    end
  end

  defp parse_ok_response(map, command) do
    response = Map.get(map, command, "")

    if String.downcase(String.trim(response)) == "ok" do
      :ok
    else
      {:error,
       Error.from_result_code(:server_error,
         message: "unexpected info response: #{inspect(response)}"
       )}
    end
  end

  defp read_udf_content(path_or_content) do
    if String.ends_with?(path_or_content, ".lua") and File.exists?(path_or_content) do
      File.read(path_or_content)
    else
      {:ok, path_or_content}
    end
  end

  defp parse_register_udf_response(map, command, conn_name, server_name) do
    response = Map.get(map, command, "")

    if String.downcase(String.trim(response)) == "ok" do
      {:ok, %RegisterTask{conn: conn_name, package_name: server_name}}
    else
      {:error,
       Error.from_result_code(:server_error,
         message: "unexpected info response: #{inspect(response)}"
       )}
    end
  end

  defp check_ready(conn_name) do
    case :ets.lookup(Tables.meta(conn_name), Tables.ready_key()) do
      [{_, true}] -> :ok
      _ -> {:error, Error.from_result_code(:cluster_not_ready)}
    end
  end

  defp with_telemetry(command, conn, fun) when is_atom(command) and is_atom(conn) do
    meta = %{command: command, conn: conn}

    :telemetry.span([:aerospike, :command], meta, fn ->
      {result, node} = fun.()
      stop = %{result: telemetry_result(result)}
      stop = if node, do: Map.put(stop, :node, node), else: stop
      {result, stop}
    end)
  end

  defp telemetry_result(:ok), do: :ok
  defp telemetry_result({:ok, _}), do: :ok
  defp telemetry_result({:error, %Error{code: code}}), do: {:error, code}
end
