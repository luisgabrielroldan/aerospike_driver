defmodule Aerospike.Protocol.Admin do
  @moduledoc false

  alias Aerospike.Privilege
  alias Aerospike.Protocol.Message
  alias Aerospike.Protocol.ResultCode
  alias Aerospike.Role
  alias Aerospike.User

  @create_user 1
  @drop_user 2
  @set_password 3
  @change_password 4
  @grant_roles 5
  @revoke_roles 6
  @query_users 9
  @create_role 10
  @drop_role 11
  @grant_privileges 12
  @revoke_privileges 13
  @query_roles 16

  @user 0
  @password 1
  @old_password 2
  @roles 10
  @role 11
  @privileges 12
  @whitelist 13
  @read_quota 14
  @write_quota 15
  @read_info 16
  @write_info 17
  @connections 18

  @msg_version 2
  @msg_type 2
  @query_end 50

  @global_privilege_codes [
    :user_admin,
    :sys_admin,
    :data_admin,
    :udf_admin,
    :sindex_admin,
    :masking_admin
  ]
  @scoped_privilege_codes [
    :read,
    :read_write,
    :read_write_udf,
    :write,
    :truncate,
    :read_masked,
    :write_masked
  ]

  @spec encode_create_user(String.t(), binary(), [String.t()]) :: binary()
  def encode_create_user(user, credential, roles)
      when is_binary(user) and is_binary(credential) and is_list(roles) do
    encode_command(@create_user, [
      encode_field(@user, user),
      encode_field(@password, credential),
      encode_roles_field(roles)
    ])
  end

  @spec encode_drop_user(String.t()) :: binary()
  def encode_drop_user(user) when is_binary(user) do
    encode_command(@drop_user, [encode_field(@user, user)])
  end

  @spec encode_set_password(String.t(), binary()) :: binary()
  def encode_set_password(user, credential) when is_binary(user) and is_binary(credential) do
    encode_command(@set_password, [encode_field(@user, user), encode_field(@password, credential)])
  end

  @spec encode_change_password(String.t(), binary(), binary()) :: binary()
  def encode_change_password(user, old_credential, new_credential)
      when is_binary(user) and is_binary(old_credential) and is_binary(new_credential) do
    encode_command(@change_password, [
      encode_field(@user, user),
      encode_field(@old_password, old_credential),
      encode_field(@password, new_credential)
    ])
  end

  @spec encode_grant_roles(String.t(), [String.t()]) :: binary()
  def encode_grant_roles(user, roles) when is_binary(user) and is_list(roles) do
    encode_command(@grant_roles, [encode_field(@user, user), encode_roles_field(roles)])
  end

  @spec encode_revoke_roles(String.t(), [String.t()]) :: binary()
  def encode_revoke_roles(user, roles) when is_binary(user) and is_list(roles) do
    encode_command(@revoke_roles, [encode_field(@user, user), encode_roles_field(roles)])
  end

  @spec encode_query_users() :: binary()
  def encode_query_users do
    encode_command(@query_users, [])
  end

  @spec encode_query_users(String.t()) :: binary()
  def encode_query_users(user) when is_binary(user) do
    encode_command(@query_users, [encode_field(@user, user)])
  end

  @spec encode_create_role(
          String.t(),
          [Privilege.t()],
          [String.t()],
          non_neg_integer(),
          non_neg_integer()
        ) ::
          {:ok, binary()} | {:error, term()}
  def encode_create_role(role_name, privileges, whitelist, read_quota, write_quota)
      when is_binary(role_name) and is_list(privileges) and is_list(whitelist) and
             is_integer(read_quota) and read_quota >= 0 and is_integer(write_quota) and
             write_quota >= 0 do
    with {:ok, privileges_field} <- encode_privileges_field(privileges) do
      fields =
        [encode_field(@role, role_name)]
        |> maybe_append(privileges != [], privileges_field)
        |> maybe_append(whitelist != [], encode_whitelist_field(whitelist))
        |> maybe_append(read_quota > 0, encode_uint32_field(@read_quota, read_quota))
        |> maybe_append(write_quota > 0, encode_uint32_field(@write_quota, write_quota))

      {:ok, encode_command(@create_role, fields)}
    end
  end

  @spec encode_drop_role(String.t()) :: binary()
  def encode_drop_role(role_name) when is_binary(role_name) do
    encode_command(@drop_role, [encode_field(@role, role_name)])
  end

  @spec encode_grant_privileges(String.t(), [Privilege.t()]) :: {:ok, binary()} | {:error, term()}
  def encode_grant_privileges(role_name, privileges)
      when is_binary(role_name) and is_list(privileges) do
    with {:ok, privileges_field} <- encode_privileges_field(privileges) do
      {:ok, encode_command(@grant_privileges, [encode_field(@role, role_name), privileges_field])}
    end
  end

  @spec encode_revoke_privileges(String.t(), [Privilege.t()]) ::
          {:ok, binary()} | {:error, term()}
  def encode_revoke_privileges(role_name, privileges)
      when is_binary(role_name) and is_list(privileges) do
    with {:ok, privileges_field} <- encode_privileges_field(privileges) do
      {:ok,
       encode_command(@revoke_privileges, [encode_field(@role, role_name), privileges_field])}
    end
  end

  @spec encode_query_roles() :: binary()
  def encode_query_roles do
    encode_command(@query_roles, [])
  end

  @spec encode_query_roles(String.t()) :: binary()
  def encode_query_roles(role_name) when is_binary(role_name) do
    encode_command(@query_roles, [encode_field(@role, role_name)])
  end

  @spec decode_admin_body(binary()) ::
          {:ok, %{result_code: atom(), raw: non_neg_integer()}} | {:error, atom()}
  def decode_admin_body(<<_::8, raw_rc::8, _::binary>>) do
    case ResultCode.from_integer(raw_rc) do
      {:ok, atom} -> {:ok, %{result_code: atom, raw: raw_rc}}
      {:error, code} -> {:ok, %{result_code: :unknown_result_code, raw: code}}
    end
  end

  def decode_admin_body(_body), do: {:error, :short_body}

  @spec decode_users_block(binary()) ::
          {:ok, %{done?: boolean(), users: [User.t()]}}
          | {:error, {:result_code, atom(), non_neg_integer()} | atom()}
  def decode_users_block(body) when is_binary(body) do
    parse_users_block(body, [])
  end

  @spec decode_roles_block(binary()) ::
          {:ok, %{done?: boolean(), roles: [Role.t()]}}
          | {:error,
             {:result_code, atom(), non_neg_integer()}
             | {:unknown_privilege_code, non_neg_integer()}
             | atom()}
  def decode_roles_block(body) when is_binary(body) do
    parse_roles_block(body, [])
  end

  defp encode_command(command, fields) do
    payload = [<<0, 0, command, length(fields), 0::96>>, fields] |> IO.iodata_to_binary()
    Message.encode(@msg_version, @msg_type, payload)
  end

  defp encode_field(id, value) when is_integer(id) and is_binary(value) do
    <<byte_size(value) + 1::32-big, id::8, value::binary>>
  end

  defp encode_uint32_field(id, value) when is_integer(id) and is_integer(value) and value >= 0 do
    <<5::32-big, id::8, value::32-big>>
  end

  defp encode_roles_field(roles) when is_list(roles) do
    encoded_roles = [<<length(roles)::8>>, Enum.map(roles, &encode_counted_string/1)]
    encode_field(@roles, IO.iodata_to_binary(encoded_roles))
  end

  defp encode_counted_string(value) when is_binary(value) do
    <<byte_size(value)::8, value::binary>>
  end

  defp encode_privileges_field(privileges) when is_list(privileges) do
    with {:ok, encoded_privileges} <- encode_privileges(privileges, []) do
      payload = IO.iodata_to_binary([<<length(privileges)>> | Enum.reverse(encoded_privileges)])
      {:ok, encode_field(@privileges, payload)}
    end
  end

  defp encode_privileges([], acc), do: {:ok, acc}

  defp encode_privileges([%Privilege{} = privilege | rest], acc) do
    with {:ok, encoded_privilege} <- encode_privilege(privilege) do
      encode_privileges(rest, [encoded_privilege | acc])
    end
  end

  defp encode_privilege(%Privilege{code: code} = privilege)
       when code in @global_privilege_codes do
    if present_scope?(privilege) do
      {:error, {:invalid_privilege_scope, privilege}}
    else
      {:ok, <<privilege_code_to_wire(code)>>}
    end
  end

  defp encode_privilege(%Privilege{code: code, namespace: namespace, set: set} = privilege)
       when code in @scoped_privilege_codes do
    namespace = namespace || ""
    set = set || ""

    if set != "" and namespace == "" do
      {:error, {:invalid_privilege_scope, privilege}}
    else
      {:ok,
       [
         <<privilege_code_to_wire(code), byte_size(namespace)>>,
         namespace,
         <<byte_size(set)>>,
         set
       ]}
    end
  end

  defp encode_privilege(%Privilege{code: code}) do
    {:error, {:invalid_privilege_code, code}}
  end

  defp encode_whitelist_field(whitelist) when is_list(whitelist) do
    whitelist
    |> Enum.join(",")
    |> then(&encode_field(@whitelist, &1))
  end

  defp maybe_append(fields, true, field), do: fields ++ [field]
  defp maybe_append(fields, false, _field), do: fields

  defp present_scope?(%Privilege{namespace: namespace, set: set}) do
    namespace not in [nil, ""] or set not in [nil, ""]
  end

  defp parse_users_block(<<>>, acc), do: {:ok, %{done?: false, users: Enum.reverse(acc)}}

  defp parse_users_block(body, _acc) when byte_size(body) < 16,
    do: {:error, :truncated_record_header}

  defp parse_users_block(
         <<_::8, raw_rc::8, _command::8, field_count::8, _::binary-size(12), rest::binary>>,
         acc
       ) do
    case decode_result_code(raw_rc) do
      :ok -> parse_next_user(field_count, rest, acc)
      :query_end -> {:ok, %{done?: true, users: Enum.reverse(acc)}}
      other -> {:error, {:result_code, other, raw_rc}}
    end
  end

  defp parse_roles_block(<<>>, acc), do: {:ok, %{done?: false, roles: Enum.reverse(acc)}}

  defp parse_roles_block(body, _acc) when byte_size(body) < 16,
    do: {:error, :truncated_record_header}

  defp parse_roles_block(
         <<_::8, raw_rc::8, _command::8, field_count::8, _::binary-size(12), rest::binary>>,
         acc
       ) do
    case decode_result_code(raw_rc) do
      :ok -> parse_next_role(field_count, rest, acc)
      :query_end -> {:ok, %{done?: true, roles: Enum.reverse(acc)}}
      other -> {:error, {:result_code, other, raw_rc}}
    end
  end

  defp parse_next_user(field_count, rest, acc) do
    with {:ok, user, tail} <- parse_user_fields(field_count, rest, %User{roles: []}) do
      acc = if empty_user?(user), do: acc, else: [user | acc]
      parse_users_block(tail, acc)
    end
  end

  defp parse_next_role(field_count, rest, acc) do
    with {:ok, role, tail} <-
           parse_role_fields(field_count, rest, %Role{name: "", privileges: []}) do
      acc = if empty_role?(role), do: acc, else: [role | acc]
      parse_roles_block(tail, acc)
    end
  end

  defp parse_user_fields(0, data, user), do: {:ok, user, data}

  defp parse_user_fields(field_count, <<flen::32-big, id::8, rest::binary>>, user)
       when field_count > 0 and flen > 0 do
    data_len = flen - 1

    case rest do
      <<field_data::binary-size(data_len), tail::binary>> ->
        with {:ok, user} <- apply_user_field(id, field_data, user) do
          parse_user_fields(field_count - 1, tail, user)
        end

      _ ->
        {:error, :truncated_field_data}
    end
  end

  defp parse_user_fields(field_count, _data, _user) when field_count > 0,
    do: {:error, :truncated_field_header}

  defp parse_role_fields(0, data, role), do: {:ok, role, data}

  defp parse_role_fields(field_count, <<flen::32-big, id::8, rest::binary>>, role)
       when field_count > 0 and flen > 0 do
    data_len = flen - 1

    case rest do
      <<field_data::binary-size(data_len), tail::binary>> ->
        with {:ok, role} <- apply_role_field(id, field_data, role) do
          parse_role_fields(field_count - 1, tail, role)
        end

      _ ->
        {:error, :truncated_field_data}
    end
  end

  defp parse_role_fields(field_count, _data, _role) when field_count > 0,
    do: {:error, :truncated_field_header}

  defp apply_user_field(@user, field_data, user), do: {:ok, %{user | name: field_data}}

  defp apply_user_field(@roles, field_data, user) do
    with {:ok, roles} <- parse_string_list(field_data) do
      {:ok, %{user | roles: roles}}
    end
  end

  defp apply_user_field(@read_info, field_data, user) do
    with {:ok, info} <- parse_info_list(field_data) do
      {:ok, %{user | read_info: info}}
    end
  end

  defp apply_user_field(@write_info, field_data, user) do
    with {:ok, info} <- parse_info_list(field_data) do
      {:ok, %{user | write_info: info}}
    end
  end

  defp apply_user_field(@connections, <<connections::32-big>>, user) do
    {:ok, %{user | connections_in_use: connections}}
  end

  defp apply_user_field(@connections, _field_data, _user),
    do: {:error, :invalid_connections_field}

  defp apply_user_field(_id, _field_data, user), do: {:ok, user}

  defp apply_role_field(@role, field_data, role), do: {:ok, %{role | name: field_data}}

  defp apply_role_field(@privileges, field_data, role) do
    with {:ok, privileges} <- parse_privileges(field_data, []) do
      {:ok, %{role | privileges: privileges}}
    end
  end

  defp apply_role_field(@whitelist, field_data, role) do
    {:ok, %{role | whitelist: parse_whitelist(field_data)}}
  end

  defp apply_role_field(@read_quota, <<read_quota::32-big>>, role) do
    {:ok, %{role | read_quota: read_quota}}
  end

  defp apply_role_field(@read_quota, _field_data, _role), do: {:error, :invalid_read_quota_field}

  defp apply_role_field(@write_quota, <<write_quota::32-big>>, role) do
    {:ok, %{role | write_quota: write_quota}}
  end

  defp apply_role_field(@write_quota, _field_data, _role),
    do: {:error, :invalid_write_quota_field}

  defp apply_role_field(_id, _field_data, role), do: {:ok, role}

  defp parse_string_list(<<count::8, rest::binary>>) do
    parse_counted_strings(rest, count, [])
  end

  defp parse_string_list(_field_data), do: {:error, :invalid_string_list_field}

  defp parse_counted_strings(rest, 0, acc) do
    if rest == <<>> do
      {:ok, Enum.reverse(acc)}
    else
      {:error, :invalid_string_list_field}
    end
  end

  defp parse_counted_strings(<<len::8, rest::binary>>, remaining, acc) when remaining > 0 do
    case rest do
      <<value::binary-size(len), tail::binary>> ->
        parse_counted_strings(tail, remaining - 1, [value | acc])

      _ ->
        {:error, :truncated_string_list_field}
    end
  end

  defp parse_counted_strings(_rest, _remaining, _acc),
    do: {:error, :truncated_string_list_field}

  defp parse_info_list(<<count::8, rest::binary>>) do
    parse_info_entries(rest, count, [])
  end

  defp parse_info_list(_field_data), do: {:error, :invalid_info_field}

  defp parse_info_entries(rest, 0, acc) do
    if rest == <<>> do
      {:ok, Enum.reverse(acc)}
    else
      {:error, :invalid_info_field}
    end
  end

  defp parse_info_entries(<<value::32-big, rest::binary>>, remaining, acc) when remaining > 0 do
    parse_info_entries(rest, remaining - 1, [value | acc])
  end

  defp parse_info_entries(_rest, _remaining, _acc), do: {:error, :truncated_info_field}

  defp parse_privileges(<<count::8, rest::binary>>, acc) do
    parse_privileges_entries(rest, count, acc)
  end

  defp parse_privileges(_field_data, _acc), do: {:error, :invalid_privileges_field}

  defp parse_privileges_entries(rest, 0, acc) do
    if rest == <<>> do
      {:ok, Enum.reverse(acc)}
    else
      {:error, :invalid_privileges_field}
    end
  end

  defp parse_privileges_entries(<<raw_code::8, rest::binary>>, remaining, acc)
       when remaining > 0 do
    case privilege_from_wire(raw_code) do
      {:ok, code} when code in @global_privilege_codes ->
        parse_privileges_entries(rest, remaining - 1, [%Privilege{code: code} | acc])

      {:ok, code} when code in @scoped_privilege_codes ->
        with {:ok, privilege, tail} <- parse_scoped_privilege(code, rest) do
          parse_privileges_entries(tail, remaining - 1, [privilege | acc])
        end

      {:error, :unknown_privilege_code} ->
        {:error, {:unknown_privilege_code, raw_code}}
    end
  end

  defp parse_privileges_entries(_rest, _remaining, _acc),
    do: {:error, :truncated_privileges_field}

  defp parse_scoped_privilege(code, <<namespace_len::8, rest::binary>>) do
    case rest do
      <<namespace::binary-size(namespace_len), set_len::8, rest2::binary>> ->
        case rest2 do
          <<set::binary-size(set_len), tail::binary>> ->
            privilege = %Privilege{
              code: code,
              namespace: empty_to_nil(namespace),
              set: empty_to_nil(set)
            }

            {:ok, privilege, tail}

          _ ->
            {:error, :truncated_privilege_scope}
        end

      _ ->
        {:error, :truncated_privilege_scope}
    end
  end

  defp parse_scoped_privilege(_code, _rest), do: {:error, :truncated_privilege_scope}

  defp parse_whitelist(field_data) when is_binary(field_data) do
    field_data
    |> :binary.split(",", [:global])
    |> Enum.reject(&(&1 == ""))
  end

  defp empty_user?(%User{name: nil, roles: []}), do: true
  defp empty_user?(%User{}), do: false

  defp empty_role?(%Role{name: "", privileges: []}), do: true
  defp empty_role?(%Role{}), do: false

  defp empty_to_nil(""), do: nil
  defp empty_to_nil(value), do: value

  defp decode_result_code(@query_end), do: :query_end

  defp decode_result_code(raw_rc) do
    case ResultCode.from_integer(raw_rc) do
      {:ok, atom} -> atom
      {:error, code} -> {:unknown_result_code, code}
    end
  end

  defp privilege_code_to_wire(:user_admin), do: 0
  defp privilege_code_to_wire(:sys_admin), do: 1
  defp privilege_code_to_wire(:data_admin), do: 2
  defp privilege_code_to_wire(:udf_admin), do: 3
  defp privilege_code_to_wire(:sindex_admin), do: 4
  defp privilege_code_to_wire(:read), do: 10
  defp privilege_code_to_wire(:read_write), do: 11
  defp privilege_code_to_wire(:read_write_udf), do: 12
  defp privilege_code_to_wire(:write), do: 13
  defp privilege_code_to_wire(:truncate), do: 14
  defp privilege_code_to_wire(:masking_admin), do: 15
  defp privilege_code_to_wire(:read_masked), do: 16
  defp privilege_code_to_wire(:write_masked), do: 17

  defp privilege_from_wire(0), do: {:ok, :user_admin}
  defp privilege_from_wire(1), do: {:ok, :sys_admin}
  defp privilege_from_wire(2), do: {:ok, :data_admin}
  defp privilege_from_wire(3), do: {:ok, :udf_admin}
  defp privilege_from_wire(4), do: {:ok, :sindex_admin}
  defp privilege_from_wire(10), do: {:ok, :read}
  defp privilege_from_wire(11), do: {:ok, :read_write}
  defp privilege_from_wire(12), do: {:ok, :read_write_udf}
  defp privilege_from_wire(13), do: {:ok, :write}
  defp privilege_from_wire(14), do: {:ok, :truncate}
  defp privilege_from_wire(15), do: {:ok, :masking_admin}
  defp privilege_from_wire(16), do: {:ok, :read_masked}
  defp privilege_from_wire(17), do: {:ok, :write_masked}
  defp privilege_from_wire(_), do: {:error, :unknown_privilege_code}
end
