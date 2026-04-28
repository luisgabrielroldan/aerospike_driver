defmodule Aerospike.Command.Admin do
  @moduledoc false

  alias Aerospike.Cluster
  alias Aerospike.Cluster.NodePool
  alias Aerospike.Cluster.Tender
  alias Aerospike.Error
  alias Aerospike.Exp
  alias Aerospike.IndexTask
  alias Aerospike.Policy
  alias Aerospike.Privilege
  alias Aerospike.Protocol.Admin, as: AdminProtocol
  alias Aerospike.Protocol.Filter, as: FilterCodec
  alias Aerospike.Protocol.Login
  alias Aerospike.Protocol.Message
  alias Aerospike.RegisterTask
  alias Aerospike.Role
  alias Aerospike.UDF
  alias Aerospike.User

  @pki_user_min {8, 1, 0, 0}
  @sindex_modern_min {8, 1, 0, 0}

  @spec info(GenServer.server(), String.t(), keyword()) ::
          {:ok, String.t()} | {:error, Error.t()}
  def info(cluster, command, opts)
      when is_binary(command) and is_list(opts) do
    with {:ok, policy} <- Policy.admin_info(opts),
         {:ok, node_name, handle, transport} <- pick_node(cluster),
         {:ok, response} <- checkout_info(node_name, handle, transport, [command], policy) do
      {:ok, Map.get(response, command, "")}
    end
  end

  @doc false
  @spec info_node(GenServer.server(), String.t(), String.t(), keyword()) ::
          {:ok, String.t()} | {:error, Error.t()}
  def info_node(cluster, node_name, command, opts)
      when is_binary(node_name) and is_binary(command) and is_list(opts) do
    with {:ok, policy} <- Policy.admin_info(opts),
         :ok <- validate_target_node(cluster, node_name),
         {:ok, handle} <- fetch_node_handle(cluster, node_name),
         {:ok, response} <-
           checkout_info(node_name, handle, Tender.transport(cluster), [command], policy) do
      {:ok, Map.get(response, command, "")}
    end
  end

  @spec list_udfs(GenServer.server(), keyword()) :: {:ok, [UDF.t()]} | {:error, Error.t()}
  def list_udfs(cluster, opts) when is_list(opts) do
    command = "udf-list"

    with {:ok, policy} <- Policy.admin_info(opts),
         {:ok, node_name, handle, transport} <- pick_node(cluster),
         {:ok, response} <- checkout_info(node_name, handle, transport, [command], policy) do
      response
      |> Map.get(command, "")
      |> parse_udf_inventory()
    end
  end

  @spec register_udf(GenServer.server(), String.t(), String.t(), keyword()) ::
          {:ok, RegisterTask.t()} | {:error, Error.t()}
  def register_udf(cluster, path_or_content, server_name, opts)
      when is_binary(path_or_content) and is_binary(server_name) and is_list(opts) do
    with {:ok, content} <- read_udf_content(path_or_content),
         {:ok, policy} <- Policy.admin_info(opts),
         {:ok, node_name, handle, transport} <- pick_node(cluster),
         command <- build_register_udf_command(content, server_name),
         {:ok, response} <- checkout_info(node_name, handle, transport, [command], policy) do
      parse_register_udf_response(response, command, cluster, server_name)
    end
  end

  @spec remove_udf(GenServer.server(), String.t(), keyword()) :: :ok | {:error, Error.t()}
  def remove_udf(cluster, server_name, opts)
      when is_binary(server_name) and is_list(opts) do
    command = "udf-remove:filename=#{server_name};"

    with {:ok, policy} <- Policy.admin_info(opts),
         {:ok, node_name, handle, transport} <- pick_node(cluster),
         {:ok, response} <- checkout_info(node_name, handle, transport, [command], policy) do
      parse_remove_udf_response(response, command)
    end
  end

  @spec truncate(GenServer.server(), String.t(), keyword()) :: :ok | {:error, Error.t()}
  def truncate(cluster, namespace, opts)
      when is_binary(namespace) and is_list(opts) do
    with {:ok, policy} <- Policy.admin_info(Keyword.delete(opts, :before)),
         {:ok, command} <- build_truncate_command(namespace, nil, opts),
         {:ok, node_name, handle, transport} <- pick_node(cluster),
         {:ok, response} <- checkout_info(node_name, handle, transport, [command], policy) do
      parse_truncate_response(response, command)
    end
  end

  @spec truncate(GenServer.server(), String.t(), String.t(), keyword()) ::
          :ok | {:error, Error.t()}
  def truncate(cluster, namespace, set, opts)
      when is_binary(namespace) and is_binary(set) and is_list(opts) do
    with {:ok, policy} <- Policy.admin_info(Keyword.delete(opts, :before)),
         {:ok, command} <- build_truncate_command(namespace, set, opts),
         {:ok, node_name, handle, transport} <- pick_node(cluster),
         {:ok, response} <- checkout_info(node_name, handle, transport, [command], policy) do
      parse_truncate_response(response, command)
    end
  end

  @spec set_xdr_filter(GenServer.server(), String.t(), String.t(), Exp.t() | nil) ::
          :ok | {:error, Error.t()}
  def set_xdr_filter(cluster, datacenter, namespace, filter)
      when is_binary(datacenter) and is_binary(namespace) do
    with {:ok, policy} <- Policy.admin_info([]),
         {:ok, command} <- build_xdr_filter_command(datacenter, namespace, filter),
         {:ok, node_name, handle, transport} <- pick_node(cluster),
         {:ok, response} <- checkout_info(node_name, handle, transport, [command], policy) do
      parse_xdr_filter_response(response, command)
    end
  end

  @spec create_user(GenServer.server(), String.t(), String.t(), [String.t()], keyword()) ::
          :ok | {:error, Error.t()}
  def create_user(cluster, user_name, password, roles, opts)
      when is_binary(user_name) and is_binary(password) and is_list(roles) and is_list(opts) do
    wire = AdminProtocol.encode_create_user(user_name, Login.hash_password(password), roles)
    execute_security_command(cluster, wire, opts)
  end

  @spec create_pki_user(GenServer.server(), String.t(), [String.t()], keyword()) ::
          :ok | {:error, Error.t()}
  def create_pki_user(cluster, user_name, roles, opts)
      when is_binary(user_name) and is_list(roles) and is_list(opts) do
    with {:ok, security_policy} <- Policy.security_admin(opts),
         {:ok, info_policy} <- Policy.admin_info(opts),
         {:ok, node_name, handle, transport} <- pick_node(cluster),
         {:ok, server_version} <- fetch_server_version(node_name, handle, transport, info_policy),
         :ok <- ensure_pki_user_supported(server_version),
         wire <-
           AdminProtocol.encode_create_pki_user(
             user_name,
             Login.no_password_credential(),
             roles
           ),
         {:ok, body} <- checkout_admin(node_name, handle, transport, wire, security_policy) do
      parse_admin_result(body)
    end
  end

  @spec drop_user(GenServer.server(), String.t(), keyword()) :: :ok | {:error, Error.t()}
  def drop_user(cluster, user_name, opts)
      when is_binary(user_name) and is_list(opts) do
    execute_security_command(cluster, AdminProtocol.encode_drop_user(user_name), opts)
  end

  @spec change_password(GenServer.server(), String.t(), String.t(), keyword()) ::
          :ok | {:error, Error.t()}
  def change_password(cluster, user_name, password, opts)
      when is_binary(user_name) and is_binary(password) and is_list(opts) do
    new_credential = Login.hash_password(password)
    %{user: auth_user, password: current_password} = Tender.auth_credentials(cluster)

    {wire, rotate?} =
      case {auth_user, current_password} do
        {^user_name, current_password} when is_binary(current_password) ->
          old_credential = Login.hash_password(current_password)
          {AdminProtocol.encode_change_password(user_name, old_credential, new_credential), true}

        _ ->
          {AdminProtocol.encode_set_password(user_name, new_credential), false}
      end

    case execute_security_command(cluster, wire, opts) do
      :ok ->
        if rotate? do
          :ok = Tender.rotate_auth_credentials(cluster, user_name, password)
        end

        :ok

      {:error, %Error{} = error} ->
        {:error, error}
    end
  end

  @spec grant_roles(GenServer.server(), String.t(), [String.t()], keyword()) ::
          :ok | {:error, Error.t()}
  def grant_roles(cluster, user_name, roles, opts)
      when is_binary(user_name) and is_list(roles) and is_list(opts) do
    execute_security_command(cluster, AdminProtocol.encode_grant_roles(user_name, roles), opts)
  end

  @spec revoke_roles(GenServer.server(), String.t(), [String.t()], keyword()) ::
          :ok | {:error, Error.t()}
  def revoke_roles(cluster, user_name, roles, opts)
      when is_binary(user_name) and is_list(roles) and is_list(opts) do
    execute_security_command(cluster, AdminProtocol.encode_revoke_roles(user_name, roles), opts)
  end

  @spec query_user(GenServer.server(), String.t(), keyword()) ::
          {:ok, User.t() | nil} | {:error, Error.t()}
  def query_user(cluster, user_name, opts)
      when is_binary(user_name) and is_list(opts) do
    case execute_user_query(cluster, AdminProtocol.encode_query_users(user_name), opts) do
      {:ok, []} -> {:ok, nil}
      {:ok, [user | _]} -> {:ok, user}
      {:error, %Error{} = error} -> {:error, error}
    end
  end

  @spec query_users(GenServer.server(), keyword()) :: {:ok, [User.t()]} | {:error, Error.t()}
  def query_users(cluster, opts) when is_list(opts) do
    execute_user_query(cluster, AdminProtocol.encode_query_users(), opts)
  end

  @spec create_role(
          GenServer.server(),
          String.t(),
          [Privilege.t()],
          [String.t()],
          non_neg_integer(),
          non_neg_integer(),
          keyword()
        ) ::
          :ok | {:error, Error.t()}
  def create_role(cluster, role_name, privileges, whitelist, read_quota, write_quota, opts)
      when is_binary(role_name) and is_list(privileges) and is_list(whitelist) and
             is_list(opts) do
    create_role_with_valid_quota(
      cluster,
      role_name,
      privileges,
      whitelist,
      read_quota,
      write_quota,
      opts
    )
  end

  defp create_role_with_valid_quota(
         cluster,
         role_name,
         privileges,
         whitelist,
         read_quota,
         write_quota,
         opts
       )
       when is_integer(read_quota) and read_quota >= 0 and is_integer(write_quota) and
              write_quota >= 0 do
    case AdminProtocol.encode_create_role(
           role_name,
           privileges,
           whitelist,
           read_quota,
           write_quota
         ) do
      {:ok, wire} -> execute_security_command(cluster, wire, opts)
      {:error, reason} -> protocol_error(reason)
    end
  end

  @spec drop_role(GenServer.server(), String.t(), keyword()) :: :ok | {:error, Error.t()}
  def drop_role(cluster, role_name, opts)
      when is_binary(role_name) and is_list(opts) do
    execute_security_command(cluster, AdminProtocol.encode_drop_role(role_name), opts)
  end

  @spec set_whitelist(GenServer.server(), String.t(), [String.t()], keyword()) ::
          :ok | {:error, Error.t()}
  def set_whitelist(cluster, role_name, whitelist, opts)
      when is_binary(role_name) and is_list(whitelist) and is_list(opts) do
    execute_security_command(
      cluster,
      AdminProtocol.encode_set_whitelist(role_name, whitelist),
      opts
    )
  end

  @spec set_quotas(
          GenServer.server(),
          String.t(),
          non_neg_integer(),
          non_neg_integer(),
          keyword()
        ) ::
          :ok | {:error, Error.t()}
  def set_quotas(cluster, role_name, read_quota, write_quota, opts)
      when is_binary(role_name) and is_integer(read_quota) and read_quota >= 0 and
             is_integer(write_quota) and write_quota >= 0 and is_list(opts) do
    execute_security_command(
      cluster,
      AdminProtocol.encode_set_quotas(role_name, read_quota, write_quota),
      opts
    )
  end

  @spec grant_privileges(GenServer.server(), String.t(), [Privilege.t()], keyword()) ::
          :ok | {:error, Error.t()}
  def grant_privileges(cluster, role_name, privileges, opts)
      when is_binary(role_name) and is_list(privileges) and is_list(opts) do
    case AdminProtocol.encode_grant_privileges(role_name, privileges) do
      {:ok, wire} -> execute_security_command(cluster, wire, opts)
      {:error, reason} -> protocol_error(reason)
    end
  end

  @spec revoke_privileges(GenServer.server(), String.t(), [Privilege.t()], keyword()) ::
          :ok | {:error, Error.t()}
  def revoke_privileges(cluster, role_name, privileges, opts)
      when is_binary(role_name) and is_list(privileges) and is_list(opts) do
    case AdminProtocol.encode_revoke_privileges(role_name, privileges) do
      {:ok, wire} -> execute_security_command(cluster, wire, opts)
      {:error, reason} -> protocol_error(reason)
    end
  end

  @spec query_role(GenServer.server(), String.t(), keyword()) ::
          {:ok, Role.t() | nil} | {:error, Error.t()}
  def query_role(cluster, role_name, opts)
      when is_binary(role_name) and is_list(opts) do
    case execute_role_query(cluster, AdminProtocol.encode_query_roles(role_name), opts) do
      {:ok, []} -> {:ok, nil}
      {:ok, [role | _]} -> {:ok, role}
      {:error, %Error{} = error} -> {:error, error}
    end
  end

  @spec query_roles(GenServer.server(), keyword()) :: {:ok, [Role.t()]} | {:error, Error.t()}
  def query_roles(cluster, opts) when is_list(opts) do
    execute_role_query(cluster, AdminProtocol.encode_query_roles(), opts)
  end

  @spec create_index(GenServer.server(), String.t(), String.t(), keyword()) ::
          {:ok, IndexTask.t()} | {:error, Error.t()}
  def create_index(cluster, namespace, set, opts)
      when is_binary(namespace) and is_binary(set) and is_list(opts) do
    with {:ok, policy} <- Policy.admin_info(opts),
         {:ok, node_name, handle, transport} <- pick_node(cluster),
         {:ok, server_version} <- fetch_server_version(node_name, handle, transport, policy),
         {:ok, command} <- build_create_index_command(server_version, namespace, set, opts),
         {:ok, response} <- checkout_info(node_name, handle, transport, [command], policy) do
      parse_create_index_response(
        response,
        command,
        cluster,
        namespace,
        Keyword.fetch!(opts, :name)
      )
    end
  end

  @spec create_expression_index(GenServer.server(), String.t(), String.t(), Exp.t(), keyword()) ::
          {:ok, IndexTask.t()} | {:error, Error.t()}
  def create_expression_index(cluster, namespace, set, %Exp{} = expression, opts)
      when is_binary(namespace) and is_binary(set) and is_list(opts) do
    with {:ok, policy} <- Policy.admin_info(opts),
         {:ok, node_name, handle, transport} <- pick_node(cluster),
         {:ok, server_version} <- fetch_server_version(node_name, handle, transport, policy),
         :ok <- ensure_expression_index_supported(server_version),
         {:ok, command} <-
           build_create_expression_index_command(server_version, namespace, set, expression, opts),
         {:ok, response} <- checkout_info(node_name, handle, transport, [command], policy) do
      parse_create_index_response(
        response,
        command,
        cluster,
        namespace,
        Keyword.fetch!(opts, :name)
      )
    end
  end

  @spec index_status(GenServer.server(), String.t(), String.t(), keyword()) ::
          {:ok, String.t()} | {:error, Error.t()}
  def index_status(cluster, namespace, index_name, opts)
      when is_binary(namespace) and is_binary(index_name) and is_list(opts) do
    with {:ok, policy} <- Policy.admin_info(opts),
         {:ok, node_name, handle, transport} <- pick_node(cluster),
         {:ok, server_version} <- fetch_server_version(node_name, handle, transport, policy),
         command <- build_index_status_command(server_version, namespace, index_name),
         {:ok, response} <- checkout_info(node_name, handle, transport, [command], policy) do
      {:ok, Map.get(response, command, "")}
    end
  end

  @doc false
  @spec index_status_node(GenServer.server(), String.t(), String.t(), String.t(), keyword()) ::
          {:ok, String.t()} | {:error, Error.t()}
  def index_status_node(cluster, node_name, namespace, index_name, opts)
      when is_binary(node_name) and is_binary(namespace) and is_binary(index_name) and
             is_list(opts) do
    with {:ok, policy} <- Policy.admin_info(opts),
         :ok <- validate_target_node(cluster, node_name),
         {:ok, handle} <- fetch_node_handle(cluster, node_name),
         transport <- Tender.transport(cluster),
         {:ok, server_version} <- fetch_server_version(node_name, handle, transport, policy),
         command <- build_index_status_command(server_version, namespace, index_name),
         {:ok, response} <- checkout_info(node_name, handle, transport, [command], policy) do
      {:ok, Map.get(response, command, "")}
    end
  end

  @spec drop_index(GenServer.server(), String.t(), String.t(), keyword()) ::
          :ok | {:error, Error.t()}
  def drop_index(cluster, namespace, index_name, opts)
      when is_binary(namespace) and is_binary(index_name) and is_list(opts) do
    with {:ok, policy} <- Policy.admin_info(opts),
         {:ok, node_name, handle, transport} <- pick_node(cluster),
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

  defp checkout_admin(node_name, handle, transport, wire, %Policy.SecurityAdmin{} = policy) do
    NodePool.checkout!(
      node_name,
      handle.pool,
      fn conn ->
        {transport.command(conn, wire, Policy.security_admin_timeout(policy),
           message_type: :admin
         ), conn}
      end,
      Policy.security_admin_checkout_timeout(policy)
    )
  end

  defp checkout_admin_stream(node_name, handle, transport, wire, %Policy.SecurityAdmin{} = policy) do
    NodePool.checkout!(
      node_name,
      handle.pool,
      fn conn ->
        {transport.command_stream(conn, wire, Policy.security_admin_timeout(policy),
           message_type: :admin
         ), conn}
      end,
      Policy.security_admin_checkout_timeout(policy)
    )
  end

  defp validate_target_node(cluster, node_name) do
    cond do
      not cluster_ready?(cluster) ->
        {:error, Error.from_result_code(:cluster_not_ready)}

      not Cluster.active_node?(cluster, node_name) ->
        {:error, Error.from_result_code(:invalid_node, node: node_name)}

      true ->
        :ok
    end
  end

  defp fetch_node_handle(cluster, node_name) do
    case Tender.node_handle(cluster, node_name) do
      {:ok, handle} ->
        {:ok, handle}

      {:error, :unknown_node} ->
        {:error, Error.from_result_code(:invalid_node, node: node_name)}
    end
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

  defp build_create_expression_index_command(server_version, namespace, set, expression, opts) do
    with {:ok, name} <- required_string(opts, :name, "expression-backed index name"),
         {:ok, type} <- expression_index_type(opts),
         {:ok, collection} <- collection_type(opts),
         {:ok, encoded} <- encode_expression_base64(expression) do
      command =
        (create_index_command_prefix(server_version) <> namespace)
        |> maybe_append_set(set)
        |> Kernel.<>(";indexname=#{name};exp=#{encoded}")
        |> maybe_append_collection(collection)
        |> Kernel.<>(";type=#{index_type_string(type)}")

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

  defp ensure_expression_index_supported(server_version) when is_tuple(server_version) do
    if modern_sindex_command?(server_version) do
      :ok
    else
      {:error,
       Error.from_result_code(:parameter_error,
         message: "expression-backed secondary indexes require Aerospike server 8.1.0 or newer"
       )}
    end
  end

  defp ensure_pki_user_supported(server_version) when is_tuple(server_version) do
    if server_version >= @pki_user_min do
      :ok
    else
      {:error,
       Error.from_result_code(:parameter_error,
         message: "PKI user creation requires Aerospike server 8.1.0 or newer"
       )}
    end
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

  defp build_truncate_command(namespace, nil, opts) when is_binary(namespace) do
    with {:ok, before} <- truncate_before(opts) do
      command = "truncate-namespace:namespace=#{namespace}"
      {:ok, maybe_append_truncate_before(command, before)}
    end
  end

  defp build_truncate_command(namespace, set, opts)
       when is_binary(namespace) and is_binary(set) do
    with {:ok, before} <- truncate_before(opts) do
      command = "truncate:namespace=#{namespace};set=#{set}"
      {:ok, maybe_append_truncate_before(command, before)}
    end
  end

  defp build_xdr_filter_command(datacenter, namespace, nil)
       when is_binary(datacenter) and is_binary(namespace) do
    {:ok, "xdr-set-filter:dc=#{datacenter};namespace=#{namespace};exp=null"}
  end

  defp build_xdr_filter_command(datacenter, namespace, %Exp{} = filter)
       when is_binary(datacenter) and is_binary(namespace) do
    with {:ok, encoded} <-
           encode_expression_base64(
             filter,
             "XDR filters require a %Aerospike.Exp{} with non-empty wire bytes"
           ) do
      {:ok, "xdr-set-filter:dc=#{datacenter};namespace=#{namespace};exp=#{encoded}"}
    end
  end

  defp build_xdr_filter_command(_datacenter, _namespace, filter) do
    {:error,
     Error.from_result_code(:invalid_argument,
       message:
         "XDR filters require nil or a %Aerospike.Exp{} with non-empty wire bytes, got: #{inspect(filter)}"
     )}
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

  defp parse_truncate_response(response, command) do
    body = Map.get(response, command, "")

    if String.downcase(String.trim(body)) == "ok" do
      :ok
    else
      {:error,
       Error.from_result_code(:server_error,
         message: "unexpected info response: #{inspect(body)}"
       )}
    end
  end

  defp parse_xdr_filter_response(response, command) do
    body = Map.get(response, command, "")

    if String.downcase(String.trim(body)) == "ok" do
      :ok
    else
      {:error,
       Error.from_result_code(:server_error,
         message: "unexpected info response: #{inspect(body)}"
       )}
    end
  end

  @doc false
  @spec parse_udf_inventory(String.t()) :: {:ok, [UDF.t()]} | {:error, Error.t()}
  def parse_udf_inventory(response) when is_binary(response) do
    response
    |> String.split(";", trim: true)
    |> Enum.reduce_while({:ok, []}, fn entry, {:ok, acc} ->
      case parse_udf_entry(entry) do
        {:ok, %UDF{} = udf} -> {:cont, {:ok, [udf | acc]}}
        :skip -> {:cont, {:ok, acc}}
        {:error, %Error{} = err} -> {:halt, {:error, err}}
      end
    end)
    |> case do
      {:ok, udfs} -> {:ok, Enum.reverse(udfs)}
      {:error, %Error{} = err} -> {:error, err}
    end
  end

  defp index_type(opts) do
    case Keyword.fetch(opts, :type) do
      {:ok, type} when type in [:numeric, :string, :geo2dsphere] ->
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

  defp expression_index_type(opts) do
    case Keyword.fetch(opts, :type) do
      {:ok, type} when type in [:numeric, :string, :geo2dsphere] ->
        {:ok, type}

      {:ok, other} ->
        {:error,
         Error.from_result_code(:parameter_error,
           message: "unsupported expression-backed index type: #{inspect(other)}"
         )}

      :error ->
        {:error,
         Error.from_result_code(:parameter_error, message: "missing required option :type")}
    end
  end

  defp encode_expression_base64(%Exp{} = expression) do
    encode_expression_base64(
      expression,
      "expression-backed secondary indexes require a %Aerospike.Exp{} with non-empty wire bytes"
    )
  end

  defp encode_expression_base64(%Exp{} = expression, empty_message)
       when is_binary(empty_message) do
    case Exp.base64(expression) do
      {:ok, encoded} ->
        {:ok, encoded}

      {:error, :empty} ->
        {:error,
         Error.from_result_code(:invalid_argument,
           message: empty_message
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

  defp build_register_udf_command(content, server_name)
       when is_binary(content) and is_binary(server_name) do
    encoded = Base.encode64(content)

    "udf-put:filename=#{server_name};content=#{encoded};content-len=#{byte_size(encoded)};udf-type=LUA;"
  end

  defp parse_udf_entry(entry) when is_binary(entry) do
    trimmed = String.trim(entry)

    if trimmed == "" do
      :skip
    else
      with {:ok, pairs} <- parse_udf_pairs(trimmed),
           {:ok, filename} <- fetch_udf_value(pairs, "filename", trimmed),
           {:ok, hash} <- fetch_udf_value(pairs, "hash", trimmed),
           {:ok, language} <- fetch_udf_value(pairs, "type", trimmed) do
        {:ok, %UDF{filename: filename, hash: hash, language: language}}
      end
    end
  end

  defp parse_udf_pairs(entry) when is_binary(entry) do
    entry
    |> String.split(",", trim: true)
    |> Enum.reduce_while({:ok, %{}}, fn pair, {:ok, acc} ->
      case String.split(pair, "=", parts: 2) do
        [key, value] when key != "" and value != "" ->
          {:cont, {:ok, Map.put(acc, key, value)}}

        _ ->
          {:halt, {:error, malformed_udf_inventory(entry)}}
      end
    end)
  end

  defp fetch_udf_value(pairs, key, entry)
       when is_map(pairs) and is_binary(key) and is_binary(entry) do
    case Map.fetch(pairs, key) do
      {:ok, value} -> {:ok, value}
      :error -> {:error, malformed_udf_inventory(entry, "missing #{key}")}
    end
  end

  defp malformed_udf_inventory(entry, detail \\ "invalid key/value pair") do
    Error.from_result_code(:server_error,
      message: "invalid udf-list entry (#{detail}): #{inspect(entry)}"
    )
  end

  defp read_udf_content(path_or_content) do
    cond do
      File.regular?(path_or_content) ->
        case File.read(path_or_content) do
          {:ok, content} ->
            {:ok, content}

          {:error, reason} ->
            {:error,
             Error.from_result_code(:invalid_argument,
               message:
                 "unable to read UDF source #{inspect(path_or_content)}: #{:file.format_error(reason)}"
             )}
        end

      String.ends_with?(path_or_content, ".lua") ->
        {:error,
         Error.from_result_code(:invalid_argument,
           message: "unable to read UDF source #{inspect(path_or_content)}: no such file"
         )}

      true ->
        {:ok, path_or_content}
    end
  end

  defp parse_register_udf_response(response, command, cluster, server_name) do
    body = Map.get(response, command, "")

    if String.downcase(String.trim(body)) == "ok" do
      {:ok, %RegisterTask{conn: cluster, package_name: server_name}}
    else
      {:error,
       Error.from_result_code(:server_error,
         message: "unexpected info response: #{inspect(body)}"
       )}
    end
  end

  defp parse_remove_udf_response(response, command) do
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

  defp execute_security_command(cluster, wire, opts) when is_binary(wire) and is_list(opts) do
    with {:ok, policy} <- Policy.security_admin(opts),
         {:ok, node_name, handle, transport} <- pick_node(cluster),
         {:ok, body} <- checkout_admin(node_name, handle, transport, wire, policy) do
      parse_admin_result(body)
    end
  end

  defp execute_user_query(cluster, wire, opts) when is_binary(wire) and is_list(opts) do
    with {:ok, policy} <- Policy.security_admin(opts),
         {:ok, node_name, handle, transport} <- pick_node(cluster),
         {:ok, frames} <- checkout_admin_stream(node_name, handle, transport, wire, policy),
         {:ok, bodies} <- decode_admin_frames(frames) do
      decode_user_bodies(bodies)
    end
  end

  defp execute_role_query(cluster, wire, opts) when is_binary(wire) and is_list(opts) do
    with {:ok, policy} <- Policy.security_admin(opts),
         {:ok, node_name, handle, transport} <- pick_node(cluster),
         {:ok, frames} <- checkout_admin_stream(node_name, handle, transport, wire, policy),
         {:ok, bodies} <- decode_admin_frames(frames) do
      decode_role_bodies(bodies)
    end
  end

  defp parse_admin_result(body) when is_binary(body) do
    case AdminProtocol.decode_admin_body(body) do
      {:ok, %{result_code: :ok}} ->
        :ok

      {:ok, %{result_code: result_code}} ->
        {:error, Error.from_result_code(result_code)}

      {:error, reason} ->
        protocol_error(reason)
    end
  end

  defp decode_admin_frames(frames) when is_binary(frames) do
    decode_admin_frames(frames, [])
  end

  defp decode_admin_frames(<<>>, acc), do: {:ok, Enum.reverse(acc)}

  defp decode_admin_frames(<<header::binary-size(8), rest::binary>>, acc) do
    with {:ok, {version, type, length}} <- Message.decode_header(header),
         true <- version == 2 or {:error, :unexpected_admin_proto_version},
         true <- type == 2 or {:error, :unexpected_admin_message_type},
         true <- byte_size(rest) >= length or {:error, :truncated_admin_frame} do
      <<body::binary-size(length), tail::binary>> = rest
      decode_admin_frames(tail, [body | acc])
    else
      {:error, _} = error -> error
      false -> {:error, :invalid_admin_frame}
    end
  end

  defp decode_admin_frames(_other, _acc), do: {:error, :truncated_admin_header}

  defp decode_user_bodies(bodies) when is_list(bodies) do
    bodies
    |> Enum.reduce_while({:ok, []}, fn body, {:ok, acc} ->
      case AdminProtocol.decode_users_block(body) do
        {:ok, %{done?: true, users: users}} ->
          {:halt, {:ok, acc ++ users}}

        {:ok, %{done?: false, users: users}} ->
          {:cont, {:ok, acc ++ users}}

        {:error, {:result_code, result_code, _raw}} ->
          {:halt, {:error, Error.from_result_code(result_code)}}

        {:error, reason} ->
          {:halt, protocol_error(reason)}
      end
    end)
  end

  defp decode_role_bodies(bodies) when is_list(bodies) do
    bodies
    |> Enum.reduce_while({:ok, []}, fn body, {:ok, acc} ->
      case AdminProtocol.decode_roles_block(body) do
        {:ok, %{done?: true, roles: roles}} ->
          {:halt, {:ok, acc ++ roles}}

        {:ok, %{done?: false, roles: roles}} ->
          {:cont, {:ok, acc ++ roles}}

        {:error, {:result_code, result_code, _raw}} ->
          {:halt, {:error, Error.from_result_code(result_code)}}

        {:error, reason} ->
          {:halt, protocol_error(reason)}
      end
    end)
  end

  defp protocol_error(reason) do
    {:error,
     Error.from_result_code(:server_error,
       message: "invalid admin response: #{inspect(reason)}"
     )}
  end

  defp truncate_before(opts) do
    case Keyword.get(opts, :before) do
      nil ->
        {:ok, nil}

      %DateTime{} = before ->
        {:ok, before}

      other ->
        {:error,
         Error.from_result_code(:invalid_argument,
           message: ":before must be a DateTime, got: #{inspect(other)}"
         )}
    end
  end

  defp maybe_append_truncate_before(command, nil), do: command

  defp maybe_append_truncate_before(command, %DateTime{} = before) do
    command <> ";lut=#{DateTime.to_unix(before, :nanosecond)}"
  end

  defp cluster_ready?(cluster) do
    Cluster.ready?(cluster)
  catch
    :exit, _ -> false
  end

  defp index_type_string(:numeric), do: "NUMERIC"
  defp index_type_string(:string), do: "STRING"
  defp index_type_string(:geo2dsphere), do: "GEO2DSPHERE"

  defp collection_type_string(:list), do: "LIST"
  defp collection_type_string(:mapkeys), do: "MAPKEYS"
  defp collection_type_string(:mapvalues), do: "MAPVALUES"
end
