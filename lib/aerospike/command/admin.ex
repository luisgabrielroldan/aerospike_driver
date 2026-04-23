defmodule Aerospike.Command.Admin do
  @moduledoc false

  alias Aerospike.Error
  alias Aerospike.Cluster
  alias Aerospike.IndexTask
  alias Aerospike.Cluster.NodePool
  alias Aerospike.Policy
  alias Aerospike.Protocol.Filter, as: FilterCodec
  alias Aerospike.Cluster.Tender

  @sindex_modern_min {8, 1, 0, 0}

  @spec create_index(GenServer.server(), String.t(), String.t(), keyword()) ::
          {:ok, IndexTask.t()} | {:error, Error.t()}
  def create_index(tender, namespace, set, opts)
      when is_binary(namespace) and is_binary(set) and is_list(opts) do
    with {:ok, policy} <- Policy.admin_info(opts),
         {:ok, node_name, handle, transport} <- pick_node(tender),
         {:ok, server_version} <- fetch_server_version(node_name, handle, transport, policy),
         {:ok, command} <- build_create_index_command(server_version, namespace, set, opts),
         {:ok, response} <- checkout_info(node_name, handle, transport, [command], policy) do
      parse_create_index_response(
        response,
        command,
        tender,
        namespace,
        Keyword.fetch!(opts, :name)
      )
    end
  end

  @spec index_status(GenServer.server(), String.t(), String.t(), keyword()) ::
          {:ok, String.t()} | {:error, Error.t()}
  def index_status(tender, namespace, index_name, opts)
      when is_binary(namespace) and is_binary(index_name) and is_list(opts) do
    with {:ok, policy} <- Policy.admin_info(opts),
         {:ok, node_name, handle, transport} <- pick_node(tender),
         {:ok, server_version} <- fetch_server_version(node_name, handle, transport, policy),
         command <- build_index_status_command(server_version, namespace, index_name),
         {:ok, response} <- checkout_info(node_name, handle, transport, [command], policy) do
      {:ok, Map.get(response, command, "")}
    end
  end

  @spec drop_index(GenServer.server(), String.t(), String.t(), keyword()) ::
          :ok | {:error, Error.t()}
  def drop_index(tender, namespace, index_name, opts)
      when is_binary(namespace) and is_binary(index_name) and is_list(opts) do
    with {:ok, policy} <- Policy.admin_info(opts),
         {:ok, node_name, handle, transport} <- pick_node(tender),
         {:ok, server_version} <- fetch_server_version(node_name, handle, transport, policy),
         command <- build_drop_index_command(server_version, namespace, index_name),
         {:ok, response} <- checkout_info(node_name, handle, transport, [command], policy) do
      parse_drop_index_response(response, command)
    end
  end

  defp pick_node(tender) do
    transport = Tender.transport(tender)

    tender
    |> Cluster.active_nodes()
    |> Enum.sort()
    |> Enum.find_value(fn node_name ->
      case Tender.node_handle(tender, node_name) do
        {:ok, handle} -> {:ok, node_name, handle, transport}
        {:error, _} -> nil
      end
    end)
    |> case do
      nil -> {:error, Error.from_result_code(:cluster_not_ready)}
      result -> result
    end
  end

  defp checkout_info(node_name, handle, transport, commands, %Policy.AdminInfo{} = policy) do
    NodePool.checkout!(
      node_name,
      handle.pool,
      fn conn ->
        {transport.info(conn, commands), conn}
      end,
      Policy.admin_checkout_timeout(policy)
    )
  end

  defp fetch_server_version(node_name, handle, transport, %Policy.AdminInfo{} = policy) do
    with {:ok, response} <- checkout_info(node_name, handle, transport, ["build"], policy) do
      parse_server_version(Map.get(response, "build", ""))
    end
  end

  defp parse_server_version(build) when is_binary(build) do
    case Regex.run(~r/^v?\d+(?:\.\d+){0,3}/, build) do
      [matched] ->
        [major, minor, patch, build_num] =
          matched
          |> String.trim_leading("v")
          |> String.split(".")
          |> Enum.map(&String.to_integer/1)
          |> Kernel.++([0, 0, 0, 0])
          |> Enum.take(4)

        {:ok, {major, minor, patch, build_num}}

      _ ->
        {:error,
         Error.from_result_code(:server_error,
           message: "unable to parse Aerospike server build for index command: #{inspect(build)}"
         )}
    end
  end

  defp build_create_index_command(server_version, namespace, set, opts) do
    with {:ok, name} <- required_string(opts, :name, "index name"),
         {:ok, bin} <- required_string(opts, :bin, "bin"),
         {:ok, type} <- index_type(opts),
         {:ok, collection} <- collection_type(opts),
         {:ok, ctx} <- context(opts) do
      command =
        (create_index_command_prefix(server_version) <> namespace)
        |> maybe_append_set(set)
        |> Kernel.<>(";indexname=#{name}")
        |> maybe_append_context(ctx)
        |> maybe_append_collection(collection)
        |> append_index_source(server_version, bin, type)

      {:ok, command}
    end
  end

  defp build_index_status_command(server_version, namespace, index_name) do
    if modern_sindex_command?(server_version) do
      "sindex-stat:namespace=#{namespace};indexname=#{index_name}"
    else
      "sindex/#{namespace}/#{index_name}"
    end
  end

  defp build_drop_index_command(server_version, namespace, index_name) do
    prefix =
      if modern_sindex_command?(server_version) do
        "sindex-delete:namespace="
      else
        "sindex-delete:ns="
      end

    prefix <> namespace <> ";indexname=#{index_name}"
  end

  defp create_index_command_prefix(server_version) do
    if modern_sindex_command?(server_version) do
      "sindex-create:namespace="
    else
      "sindex-create:ns="
    end
  end

  defp modern_sindex_command?(server_version) when is_tuple(server_version) do
    server_version >= @sindex_modern_min
  end

  defp maybe_append_set(command, ""), do: command
  defp maybe_append_set(command, set), do: command <> ";set=#{set}"

  defp maybe_append_context(command, nil), do: command

  defp maybe_append_context(command, ctx) when is_list(ctx) do
    command <> ";context=" <> (ctx |> FilterCodec.encode_ctx() |> Base.encode64())
  end

  defp maybe_append_collection(command, nil), do: command

  defp maybe_append_collection(command, collection) do
    command <> ";indextype=" <> collection_type_string(collection)
  end

  defp append_index_source(command, server_version, bin, type) do
    if modern_sindex_command?(server_version) do
      command <> ";bin=#{bin};type=" <> index_type_string(type)
    else
      command <> ";indexdata=#{bin}," <> index_type_string(type)
    end
  end

  defp parse_create_index_response(response, command, tender, namespace, index_name) do
    body = Map.get(response, command, "")

    if String.downcase(String.trim(body)) == "ok" do
      {:ok, %IndexTask{conn: tender, namespace: namespace, index_name: index_name}}
    else
      {:error,
       Error.from_result_code(:server_error,
         message: "unexpected info response: #{inspect(body)}"
       )}
    end
  end

  defp parse_drop_index_response(response, command) do
    body = Map.get(response, command, "")
    lowered = String.downcase(String.trim(body))

    cond do
      lowered == "ok" ->
        :ok

      String.contains?(lowered, "not found") ->
        :ok

      true ->
        {:error,
         Error.from_result_code(:server_error,
           message: "unexpected info response: #{inspect(body)}"
         )}
    end
  end

  defp index_type(opts) do
    case Keyword.fetch(opts, :type) do
      {:ok, type} when type in [:numeric, :string] ->
        {:ok, type}

      {:ok, other} ->
        {:error,
         Error.from_result_code(:parameter_error,
           message: "unsupported index type: #{inspect(other)}"
         )}

      :error ->
        {:error,
         Error.from_result_code(:parameter_error, message: "missing required option :type")}
    end
  end

  defp collection_type(opts) do
    case Keyword.get(opts, :collection) do
      nil ->
        {:ok, nil}

      collection when collection in [:list, :mapkeys, :mapvalues] ->
        {:ok, collection}

      other ->
        {:error,
         Error.from_result_code(:parameter_error,
           message: "unsupported collection type: #{inspect(other)}"
         )}
    end
  end

  defp context(opts) do
    case Keyword.get(opts, :ctx) do
      nil ->
        {:ok, nil}

      ctx when is_list(ctx) and ctx != [] ->
        {:ok, ctx}

      other ->
        {:error,
         Error.from_result_code(:parameter_error,
           message: "ctx must be a non-empty list, got: #{inspect(other)}"
         )}
    end
  end

  defp required_string(opts, key, label) do
    case Keyword.fetch(opts, key) do
      {:ok, value} when is_binary(value) and value != "" ->
        {:ok, value}

      {:ok, other} ->
        {:error,
         Error.from_result_code(:parameter_error,
           message: "#{label} must be a non-empty string, got: #{inspect(other)}"
         )}

      :error ->
        {:error,
         Error.from_result_code(:parameter_error, message: "missing required option :#{key}")}
    end
  end

  defp index_type_string(:numeric), do: "NUMERIC"
  defp index_type_string(:string), do: "STRING"

  defp collection_type_string(:list), do: "LIST"
  defp collection_type_string(:mapkeys), do: "MAPKEYS"
  defp collection_type_string(:mapvalues), do: "MAPVALUES"
end
