defmodule Aerospike.Admin do
  @moduledoc false

  alias Aerospike.Admin.PasswordHash
  alias Aerospike.Cluster
  alias Aerospike.Connection
  alias Aerospike.Error
  alias Aerospike.IndexTask
  alias Aerospike.Privilege
  alias Aerospike.Protocol.Admin, as: AdminProtocol
  alias Aerospike.RegisterTask
  alias Aerospike.Role
  alias Aerospike.Router
  alias Aerospike.Tables
  alias Aerospike.User

  @admin_message_type 2
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
  @spec create_user(atom(), String.t(), String.t(), [String.t()], keyword()) ::
          :ok | {:error, Error.t()}
  def create_user(conn_name, user_name, password, roles, opts)
      when is_atom(conn_name) and is_binary(user_name) and is_binary(password) and is_list(roles) and
             is_list(opts) do
    wire = AdminProtocol.encode_create_user(user_name, PasswordHash.hash(password), roles)
    execute_security_command(conn_name, :create_user, wire, opts)
  end

  @doc false
  @spec create_pki_user(atom(), String.t(), [String.t()], keyword()) :: :ok | {:error, Error.t()}
  def create_pki_user(conn_name, user_name, roles, opts)
      when is_atom(conn_name) and is_binary(user_name) and is_list(roles) and is_list(opts) do
    wire =
      AdminProtocol.encode_create_pki_user(
        user_name,
        PasswordHash.no_password_credential(),
        roles
      )

    execute_security_command(conn_name, :create_pki_user, wire, opts)
  end

  @doc false
  @spec drop_user(atom(), String.t(), keyword()) :: :ok | {:error, Error.t()}
  def drop_user(conn_name, user_name, opts)
      when is_atom(conn_name) and is_binary(user_name) and is_list(opts) do
    execute_security_command(
      conn_name,
      :drop_user,
      AdminProtocol.encode_drop_user(user_name),
      opts
    )
  end

  @doc false
  @spec change_password(atom(), String.t(), String.t(), keyword()) :: :ok | {:error, Error.t()}
  def change_password(conn_name, user_name, password, opts)
      when is_atom(conn_name) and is_binary(user_name) and is_binary(password) and is_list(opts) do
    new_credential = PasswordHash.hash(password)
    auth_opts = auth_opts(conn_name)

    {wire, self_change?} =
      case {Keyword.get(auth_opts, :user), Keyword.get(auth_opts, :credential)} do
        {^user_name, current_credential} when is_binary(current_credential) ->
          {AdminProtocol.encode_change_password(user_name, current_credential, new_credential),
           true}

        _ ->
          {AdminProtocol.encode_set_password(user_name, new_credential), false}
      end

    case execute_security_command(conn_name, :change_password, wire, opts) do
      :ok ->
        if self_change? do
          update_auth_credential(conn_name, user_name, new_credential)
        end

        :ok

      {:error, %Error{} = error} ->
        {:error, error}
    end
  end

  @doc false
  @spec grant_roles(atom(), String.t(), [String.t()], keyword()) :: :ok | {:error, Error.t()}
  def grant_roles(conn_name, user_name, roles, opts)
      when is_atom(conn_name) and is_binary(user_name) and is_list(roles) and is_list(opts) do
    execute_security_command(
      conn_name,
      :grant_roles,
      AdminProtocol.encode_grant_roles(user_name, roles),
      opts
    )
  end

  @doc false
  @spec revoke_roles(atom(), String.t(), [String.t()], keyword()) :: :ok | {:error, Error.t()}
  def revoke_roles(conn_name, user_name, roles, opts)
      when is_atom(conn_name) and is_binary(user_name) and is_list(roles) and is_list(opts) do
    execute_security_command(
      conn_name,
      :revoke_roles,
      AdminProtocol.encode_revoke_roles(user_name, roles),
      opts
    )
  end

  @doc false
  @spec query_user(atom(), String.t(), keyword()) :: {:ok, User.t() | nil} | {:error, Error.t()}
  def query_user(conn_name, user_name, opts)
      when is_atom(conn_name) and is_binary(user_name) and is_list(opts) do
    case execute_user_query(
           conn_name,
           :query_user,
           AdminProtocol.encode_query_users(user_name),
           opts
         ) do
      {:ok, []} -> {:ok, nil}
      {:ok, [user | _]} -> {:ok, user}
      {:error, %Error{} = error} -> {:error, error}
    end
  end

  @doc false
  @spec query_users(atom(), keyword()) :: {:ok, [User.t()]} | {:error, Error.t()}
  def query_users(conn_name, opts) when is_atom(conn_name) and is_list(opts) do
    execute_user_query(conn_name, :query_users, AdminProtocol.encode_query_users(), opts)
  end

  @doc false
  @spec create_role(atom(), String.t(), [Privilege.t()], keyword()) :: :ok | {:error, Error.t()}
  def create_role(conn_name, role_name, privileges, opts)
      when is_atom(conn_name) and is_binary(role_name) and is_list(privileges) and is_list(opts) do
    whitelist = Keyword.get(opts, :whitelist, [])
    read_quota = Keyword.get(opts, :read_quota, 0)
    write_quota = Keyword.get(opts, :write_quota, 0)

    case AdminProtocol.encode_create_role(
           role_name,
           privileges,
           whitelist,
           read_quota,
           write_quota
         ) do
      {:ok, wire} ->
        execute_security_command(conn_name, :create_role, wire, opts)

      {:error, reason} ->
        protocol_error(reason)
    end
  end

  @doc false
  @spec drop_role(atom(), String.t(), keyword()) :: :ok | {:error, Error.t()}
  def drop_role(conn_name, role_name, opts)
      when is_atom(conn_name) and is_binary(role_name) and is_list(opts) do
    execute_security_command(
      conn_name,
      :drop_role,
      AdminProtocol.encode_drop_role(role_name),
      opts
    )
  end

  @doc false
  @spec grant_privileges(atom(), String.t(), [Privilege.t()], keyword()) ::
          :ok | {:error, Error.t()}
  def grant_privileges(conn_name, role_name, privileges, opts)
      when is_atom(conn_name) and is_binary(role_name) and is_list(privileges) and is_list(opts) do
    case AdminProtocol.encode_grant_privileges(role_name, privileges) do
      {:ok, wire} ->
        execute_security_command(conn_name, :grant_privileges, wire, opts)

      {:error, reason} ->
        protocol_error(reason)
    end
  end

  @doc false
  @spec revoke_privileges(atom(), String.t(), [Privilege.t()], keyword()) ::
          :ok | {:error, Error.t()}
  def revoke_privileges(conn_name, role_name, privileges, opts)
      when is_atom(conn_name) and is_binary(role_name) and is_list(privileges) and is_list(opts) do
    case AdminProtocol.encode_revoke_privileges(role_name, privileges) do
      {:ok, wire} ->
        execute_security_command(conn_name, :revoke_privileges, wire, opts)

      {:error, reason} ->
        protocol_error(reason)
    end
  end

  @doc false
  @spec set_whitelist(atom(), String.t(), [String.t()], keyword()) :: :ok | {:error, Error.t()}
  def set_whitelist(conn_name, role_name, whitelist, opts)
      when is_atom(conn_name) and is_binary(role_name) and is_list(whitelist) and is_list(opts) do
    execute_security_command(
      conn_name,
      :set_whitelist,
      AdminProtocol.encode_set_whitelist(role_name, whitelist),
      opts
    )
  end

  @doc false
  @spec set_quotas(atom(), String.t(), non_neg_integer(), non_neg_integer(), keyword()) ::
          :ok | {:error, Error.t()}
  def set_quotas(conn_name, role_name, read_quota, write_quota, opts)
      when is_atom(conn_name) and is_binary(role_name) and is_integer(read_quota) and
             read_quota >= 0 and is_integer(write_quota) and write_quota >= 0 and is_list(opts) do
    wire = AdminProtocol.encode_set_quotas(role_name, read_quota, write_quota)
    execute_security_command(conn_name, :set_quotas, wire, opts)
  end

  @doc false
  @spec query_role(atom(), String.t(), keyword()) :: {:ok, Role.t() | nil} | {:error, Error.t()}
  def query_role(conn_name, role_name, opts)
      when is_atom(conn_name) and is_binary(role_name) and is_list(opts) do
    case execute_role_query(
           conn_name,
           :query_role,
           AdminProtocol.encode_query_roles(role_name),
           opts
         ) do
      {:ok, []} -> {:ok, nil}
      {:ok, [role | _]} -> {:ok, role}
      {:error, %Error{} = error} -> {:error, error}
    end
  end

  @doc false
  @spec query_roles(atom(), keyword()) :: {:ok, [Role.t()]} | {:error, Error.t()}
  def query_roles(conn_name, opts) when is_atom(conn_name) and is_list(opts) do
    execute_role_query(conn_name, :query_roles, AdminProtocol.encode_query_roles(), opts)
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

  defp execute_security_command(conn_name, command, wire, opts)
       when is_atom(conn_name) and is_atom(command) and is_binary(wire) and is_list(opts) do
    checkout_timeout = Keyword.get(opts, :pool_checkout_timeout, @default_checkout_timeout)

    with_telemetry(command, conn_name, fn ->
      with {:ok, pool_pid, node_name} <- Router.random_node_pool(conn_name),
           {:ok, body} <-
             Router.checkout_and_request(pool_pid, wire, checkout_timeout, {conn_name, node_name}) do
        {parse_admin_result(body), node_name}
      else
        {:error, %Error{} = e} -> {{:error, e}, nil}
      end
    end)
  end

  defp execute_user_query(conn_name, command, wire, opts)
       when is_atom(conn_name) and is_atom(command) and is_binary(wire) and is_list(opts) do
    execute_query(conn_name, command, wire, opts, &AdminProtocol.decode_users_block/1, :users)
  end

  defp execute_role_query(conn_name, command, wire, opts)
       when is_atom(conn_name) and is_atom(command) and is_binary(wire) and is_list(opts) do
    execute_query(conn_name, command, wire, opts, &AdminProtocol.decode_roles_block/1, :roles)
  end

  defp execute_query(conn_name, command, wire, opts, decoder, result_key)
       when is_atom(conn_name) and is_atom(command) and is_binary(wire) and is_list(opts) and
              is_function(decoder, 1) and result_key in [:users, :roles] do
    checkout_timeout = Keyword.get(opts, :pool_checkout_timeout, @default_checkout_timeout)

    with_telemetry(command, conn_name, fn ->
      with {:ok, pool_pid, node_name} <- Router.random_node_pool(conn_name),
           {:ok, bodies} <-
             checkout_and_request_admin_stream(
               pool_pid,
               wire,
               checkout_timeout,
               {conn_name, node_name}
             ) do
        {decode_query_bodies(bodies, decoder, result_key), node_name}
      else
        {:error, %Error{} = e} -> {{:error, e}, nil}
      end
    end)
  end

  defp decode_query_bodies(bodies, decoder, result_key)
       when is_list(bodies) and is_function(decoder, 1) and result_key in [:users, :roles] do
    bodies
    |> Enum.reduce_while({:ok, []}, fn body, {:ok, acc} ->
      decode_query_body(body, acc, decoder, result_key)
    end)
  end

  defp decode_query_body(body, acc, decoder, result_key) do
    case decoder.(body) do
      {:ok, %{done?: done?} = result} ->
        items = Map.fetch!(result, result_key)
        reduce_query_items(acc, items, done?)

      {:error, {:result_code, result_code, _raw}} ->
        {:halt, {:error, Error.from_result_code(result_code)}}

      {:error, {:unknown_privilege_code, code}} ->
        {:halt,
         {:error,
          Error.from_result_code(:server_error,
            message: "unknown privilege code in admin response: #{code}"
          )}}

      {:error, reason} ->
        {:halt, protocol_error(reason)}
    end
  end

  defp reduce_query_items(acc, items, true), do: {:halt, {:ok, acc ++ items}}
  defp reduce_query_items(acc, items, false), do: {:cont, {:ok, acc ++ items}}

  defp checkout_and_request_admin_stream(pool_pid, wire, checkout_timeout, breaker_ctx)
       when is_pid(pool_pid) and is_integer(checkout_timeout) and checkout_timeout >= 0 do
    NimblePool.checkout!(
      pool_pid,
      :checkout,
      fn _from, conn ->
        case send_and_receive_admin_stream(conn, wire, []) do
          {:ok, conn2, bodies} ->
            {{:ok, bodies}, conn2}

          {:error, reason} ->
            maybe_record_breaker(breaker_ctx, :network_error)
            e = Error.from_result_code(:network_error, message: inspect(reason))
            {{:error, e}, :close}
        end
      end,
      checkout_timeout
    )
  catch
    :exit, {:timeout, {NimblePool, :checkout, _}} ->
      maybe_record_breaker(breaker_ctx, :pool_timeout)
      {:error, Error.from_result_code(:pool_timeout)}

    :exit, {:noproc, {NimblePool, :checkout, _}} ->
      {:error, Error.from_result_code(:invalid_node)}

    :exit, reason ->
      maybe_record_breaker(breaker_ctx, :network_error)
      {:error, Error.from_result_code(:network_error, message: inspect(reason))}
  end

  defp send_and_receive_admin_stream(conn, wire, acc) do
    with {:ok, conn} <- Connection.send_command(conn, wire) do
      receive_admin_messages(conn, acc)
    end
  end

  defp receive_admin_messages(conn, acc) do
    with {:ok, conn, _version, type, body} <- Connection.recv_message(conn),
         :ok <- ensure_admin_message_type(type),
         {:ok, %{result_code: result_code}} <- AdminProtocol.decode_admin_body(body) do
      acc = [body | acc]

      if result_code == :ok do
        receive_admin_messages(conn, acc)
      else
        {:ok, conn, Enum.reverse(acc)}
      end
    end
  end

  defp ensure_admin_message_type(@admin_message_type), do: :ok
  defp ensure_admin_message_type(_type), do: {:error, :unexpected_message_type}

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

  defp protocol_error(reason) do
    {:error,
     Error.from_result_code(:server_error,
       message: "invalid admin response: #{inspect(reason)}"
     )}
  end

  defp auth_opts(conn_name) when is_atom(conn_name) do
    case :ets.lookup(Tables.meta(conn_name), :auth_opts) do
      [{:auth_opts, opts}] when is_list(opts) -> opts
      _ -> []
    end
  end

  defp update_auth_credential(conn_name, user_name, credential)
       when is_atom(conn_name) and is_binary(user_name) and is_binary(credential) do
    case Cluster.rotate_auth_credential(conn_name, user_name, credential) do
      :ok ->
        :ok

      {:error, :cluster_not_found} ->
        opts =
          auth_opts(conn_name)
          |> Keyword.put(:user, user_name)
          |> Keyword.put(:credential, credential)

        :ets.insert(Tables.meta(conn_name), {:auth_opts, opts})
        :ok
    end
  end

  defp maybe_record_breaker({conn_name, node_name}, reason) do
    Aerospike.CircuitBreaker.record_error(conn_name, node_name, reason)
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
