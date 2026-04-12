defmodule Aerospike.Protocol.AdminTest do
  use ExUnit.Case, async: true

  alias Aerospike.Privilege
  alias Aerospike.Protocol.Admin
  alias Aerospike.Protocol.Message
  alias Aerospike.Role
  alias Aerospike.User

  describe "simple command encoders" do
    test "encode_login/2 produces an admin frame with user and credential fields" do
      bin = Admin.encode_login("testuser", <<1, 2, 3, 4>>)
      assert {:ok, {2, 2, body}} = Message.decode(bin)

      assert_command(body, 20, 2)
      assert decode_field_ids(body) == [0, 3]
    end

    test "user command encoders use the expected command ids and field ordering" do
      commands = [
        {Admin.encode_create_user("user", "hash", ["rw"]), 1, [0, 1, 10]},
        {Admin.encode_create_pki_user("user", "nopassword-hash", ["rw"]), 1, [0, 1, 10]},
        {Admin.encode_drop_user("user"), 2, [0]},
        {Admin.encode_set_password("user", "hash"), 3, [0, 1]},
        {Admin.encode_change_password("user", "old", "new"), 4, [0, 2, 1]},
        {Admin.encode_grant_roles("user", ["rw"]), 5, [0, 10]},
        {Admin.encode_revoke_roles("user", ["rw"]), 6, [0, 10]},
        {Admin.encode_query_users(), 9, []},
        {Admin.encode_query_users("user"), 9, [0]}
      ]

      Enum.each(commands, fn {frame, command, field_ids} ->
        assert {:ok, {2, 2, body}} = Message.decode(frame)
        assert_command(body, command, length(field_ids))
        assert decode_field_ids(body) == field_ids
      end)
    end

    test "role command encoders use the expected command ids and field ordering" do
      privilege = %Privilege{code: :read_write, namespace: "test", set: "demo"}

      assert {:ok, create_role} =
               Admin.encode_create_role("analyst", [privilege], ["10.0.0.0/24"], 100, 200)

      assert {:ok, grant_privileges} = Admin.encode_grant_privileges("analyst", [privilege])
      assert {:ok, revoke_privileges} = Admin.encode_revoke_privileges("analyst", [privilege])

      commands = [
        {create_role, 10, [11, 12, 13, 14, 15]},
        {Admin.encode_drop_role("analyst"), 11, [11]},
        {grant_privileges, 12, [11, 12]},
        {revoke_privileges, 13, [11, 12]},
        {Admin.encode_set_whitelist("analyst", ["10.0.0.0/24"]), 14, [11, 13]},
        {Admin.encode_set_whitelist("analyst", []), 14, [11]},
        {Admin.encode_set_quotas("analyst", 10, 20), 15, [11, 14, 15]},
        {Admin.encode_query_roles(), 16, []},
        {Admin.encode_query_roles("analyst"), 16, [11]}
      ]

      Enum.each(commands, fn {frame, command, field_ids} ->
        assert {:ok, {2, 2, body}} = Message.decode(frame)
        assert_command(body, command, length(field_ids))
        assert decode_field_ids(body) == field_ids
      end)
    end
  end

  describe "privilege encoding validation" do
    test "rejects scoped data privileges with a set but no namespace" do
      privilege = %Privilege{code: :read, set: "demo"}

      assert {:error, {:invalid_privilege_scope, ^privilege}} =
               Admin.encode_grant_privileges("analyst", [privilege])
    end

    test "rejects global privileges with namespace scope" do
      privilege = %Privilege{code: :user_admin, namespace: "test"}

      assert {:error, {:invalid_privilege_scope, ^privilege}} =
               Admin.encode_create_role("analyst", [privilege], [], 0, 0)
    end

    test "rejects unknown privilege codes during encoding" do
      privilege = %Privilege{code: :unknown, raw_code: 99}

      assert {:error, {:invalid_privilege_code, :unknown}} =
               Admin.encode_revoke_privileges("analyst", [privilege])
    end

    test "encodes scoped privilege payloads with namespace and set lengths" do
      privilege = %Privilege{code: :read_write, namespace: "test", set: "demo"}

      assert {:ok, frame} = Admin.encode_grant_privileges("analyst", [privilege])
      {:ok, {_version, _type, body}} = Message.decode(frame)

      [{11, "analyst"}, {12, privileges_payload}] = decode_fields(body)
      assert <<1, 11, 4, "test", 4, "demo">> = privileges_payload
    end
  end

  describe "decode_admin_body/1" do
    test "reads result code at byte index 1" do
      body = :binary.copy(<<0>>, 16)
      assert {:ok, %{result_code: :ok, raw: 0}} = Admin.decode_admin_body(body)
    end

    test "maps non-zero wire code at byte index 1" do
      body = put_byte(:binary.copy(<<0>>, 16), 1, 2)
      assert {:ok, %{result_code: :key_not_found, raw: 2}} = Admin.decode_admin_body(body)
    end

    test "returns unknown_result_code for unmapped wire codes" do
      body = put_byte(:binary.copy(<<0>>, 16), 1, 254)
      assert {:ok, %{result_code: :unknown_result_code, raw: 254}} = Admin.decode_admin_body(body)
    end

    test "returns short_body error for bodies smaller than 2 bytes" do
      assert {:error, :short_body} = Admin.decode_admin_body(<<0>>)
      assert {:error, :short_body} = Admin.decode_admin_body(<<>>)
    end
  end

  describe "decode_response/1" do
    test "decodes a full admin frame" do
      body = put_byte(:binary.copy(<<0>>, 16), 1, 52)
      frame = Message.encode(2, 2, body)

      assert {:ok, %{result_code: :security_not_enabled, raw: 52}} =
               Admin.decode_response(frame)
    end
  end

  describe "decode_users_block/1" do
    test "parses a user record with roles, counters, and connections" do
      body =
        user_record_body([
          field(0, "alice"),
          field(10, counted_strings(["read-write", "sys-admin"])),
          field(16, info_values([1, 2])),
          field(17, info_values([3])),
          field(18, <<7::32-big>>)
        ])

      assert {:ok, %{done?: false, users: [%User{} = user]}} = Admin.decode_users_block(body)
      assert user.name == "alice"
      assert user.roles == ["read-write", "sys-admin"]
      assert user.read_info == [1, 2]
      assert user.write_info == [3]
      assert user.connections_in_use == 7
    end

    test "returns done on query end" do
      assert {:ok, %{done?: true, users: []}} =
               Admin.decode_users_block(admin_record(50, []))
    end

    test "returns result-code errors for non-query-end failures" do
      assert {:error, {:result_code, :invalid_user, 60}} =
               Admin.decode_users_block(admin_record(60, []))
    end

    test "ignores empty user records" do
      assert {:ok, %{done?: false, users: []}} = Admin.decode_users_block(user_record_body([]))
    end

    test "returns truncated-record-header errors for short bodies" do
      assert {:error, :truncated_record_header} = Admin.decode_users_block(<<0::size(120)>>)
    end

    test "returns truncated-field-header errors for partial user field headers" do
      body = admin_header(0, 1) <> <<0, 0, 0, 5>>
      assert {:error, :truncated_field_header} = Admin.decode_users_block(body)
    end

    test "returns truncated-field errors for partial payloads" do
      body = admin_header(0, 1) <> <<0, 0, 0, 5, 0, "abc">>
      assert {:error, :truncated_field_data} = Admin.decode_users_block(body)
    end

    test "returns invalid-string-list errors for malformed role payloads" do
      body =
        user_record_body([
          field(0, "alice"),
          field(10, <<0, 1>>)
        ])

      assert {:error, :invalid_string_list_field} = Admin.decode_users_block(body)
    end

    test "returns truncated-string-list errors for partial role payloads" do
      body =
        user_record_body([
          field(0, "alice"),
          field(10, <<1, 5, "abc">>)
        ])

      assert {:error, :truncated_string_list_field} = Admin.decode_users_block(body)
    end

    test "returns invalid-info errors for malformed counter payloads" do
      body =
        user_record_body([
          field(0, "alice"),
          field(16, <<0, 1>>)
        ])

      assert {:error, :invalid_info_field} = Admin.decode_users_block(body)
    end

    test "returns truncated-info errors for partial counter payloads" do
      body =
        user_record_body([
          field(0, "alice"),
          field(17, <<1, 0, 0, 0>>)
        ])

      assert {:error, :truncated_info_field} = Admin.decode_users_block(body)
    end

    test "returns invalid-connections-field errors for non-uint32 widths" do
      body =
        user_record_body([
          field(0, "alice"),
          field(18, <<7::16-big>>)
        ])

      assert {:error, :invalid_connections_field} = Admin.decode_users_block(body)
    end
  end

  describe "decode_roles_block/1" do
    test "parses role metadata including privileges, whitelist, and quotas" do
      privileges =
        <<2, 11, 4, "test", 4, "demo", 1>>

      body =
        role_record_body([
          field(11, "analyst"),
          field(12, privileges),
          field(13, "10.0.0.1,10.0.0.0/24"),
          field(14, <<25::32-big>>),
          field(15, <<50::32-big>>)
        ])

      assert {:ok, %{done?: false, roles: [%Role{} = role]}} = Admin.decode_roles_block(body)
      assert role.name == "analyst"

      assert role.privileges == [
               %Privilege{code: :read_write, namespace: "test", set: "demo"},
               %Privilege{code: :sys_admin, namespace: nil, set: nil}
             ]

      assert role.whitelist == ["10.0.0.1", "10.0.0.0/24"]
      assert role.read_quota == 25
      assert role.write_quota == 50
    end

    test "rejects unknown privilege codes during decode" do
      body =
        role_record_body([
          field(11, "analyst"),
          field(12, <<1, 99>>)
        ])

      assert {:error, {:unknown_privilege_code, 99}} = Admin.decode_roles_block(body)
    end

    test "returns done on query end" do
      assert {:ok, %{done?: true, roles: []}} =
               Admin.decode_roles_block(admin_record(50, []))
    end

    test "ignores empty role records" do
      assert {:ok, %{done?: false, roles: []}} = Admin.decode_roles_block(role_record_body([]))
    end

    test "returns truncated-record-header errors for short role bodies" do
      assert {:error, :truncated_record_header} = Admin.decode_roles_block(<<0::size(120)>>)
    end

    test "returns truncated-field-header errors for partial role field headers" do
      body = admin_header(0, 1) <> <<0, 0, 0, 5>>
      assert {:error, :truncated_field_header} = Admin.decode_roles_block(body)
    end

    test "returns truncated privilege scope errors" do
      body =
        role_record_body([
          field(11, "analyst"),
          field(12, <<1, 11, 4, "tes">>)
        ])

      assert {:error, :truncated_privilege_scope} = Admin.decode_roles_block(body)
    end

    test "returns invalid-privileges errors for malformed privilege payloads" do
      body =
        role_record_body([
          field(11, "analyst"),
          field(12, <<0, 1>>)
        ])

      assert {:error, :invalid_privileges_field} = Admin.decode_roles_block(body)
    end

    test "returns truncated-privileges errors for partial privilege payloads" do
      body =
        role_record_body([
          field(11, "analyst"),
          field(12, <<2, 1>>)
        ])

      assert {:error, :truncated_privileges_field} = Admin.decode_roles_block(body)
    end

    test "returns invalid read quota errors for non-uint32 widths" do
      body =
        role_record_body([
          field(11, "analyst"),
          field(14, <<25::16-big>>)
        ])

      assert {:error, :invalid_read_quota_field} = Admin.decode_roles_block(body)
    end

    test "returns invalid write quota errors for non-uint32 widths" do
      body =
        role_record_body([
          field(11, "analyst"),
          field(15, <<50::16-big>>)
        ])

      assert {:error, :invalid_write_quota_field} = Admin.decode_roles_block(body)
    end
  end

  describe "decode_login_session_fields/1" do
    test "returns nils for body <= 16 bytes" do
      body = :binary.copy(<<0>>, 16)
      assert %{session_token: nil, session_ttl_sec: nil} = Admin.decode_login_session_fields(body)
    end

    test "parses session_token and session_ttl_sec" do
      token = "tok"
      ttl = 7200
      token_field = <<byte_size(token) + 1::32-big, 5::8, token::binary>>
      ttl_field = <<5::32-big, 6::8, ttl::32-big>>
      body = :binary.copy(<<0>>, 16) <> token_field <> ttl_field

      result = Admin.decode_login_session_fields(body)
      assert result.session_token == token
      assert result.session_ttl_sec == ttl
    end

    test "ignores truncated session fields" do
      body = :binary.copy(<<0>>, 16) <> <<0, 0, 0, 10, 5>>
      result = Admin.decode_login_session_fields(body)
      assert result.session_token == nil
      assert result.session_ttl_sec == nil
    end
  end

  describe "login_command_id/0" do
    test "returns the login command constant" do
      assert Admin.login_command_id() == 20
    end
  end

  defp assert_command(body, command, field_count) do
    assert <<0, 0, ^command, ^field_count, _::binary-size(12)>> = binary_part(body, 0, 16)
  end

  defp decode_field_ids(body) do
    body
    |> decode_fields()
    |> Enum.map(fn {id, _data} -> id end)
  end

  defp decode_fields(<<_admin::binary-size(16), rest::binary>>) do
    decode_fields(rest, [])
  end

  defp decode_fields(<<>>, acc), do: Enum.reverse(acc)

  defp decode_fields(<<len::32-big, id::8, rest::binary>>, acc) do
    data_len = len - 1
    <<field_data::binary-size(data_len), tail::binary>> = rest
    decode_fields(tail, [{id, field_data} | acc])
  end

  defp admin_record(result_code, fields) do
    admin_header(result_code, length(fields)) <> IO.iodata_to_binary(fields)
  end

  defp admin_header(result_code, field_count) do
    <<0, result_code, 0, field_count>> <> <<0::size(96)>>
  end

  defp user_record_body(fields), do: admin_record(0, fields)
  defp role_record_body(fields), do: admin_record(0, fields)

  defp field(id, data) when is_integer(id) and is_binary(data) do
    <<byte_size(data) + 1::32-big, id::8, data::binary>>
  end

  defp counted_strings(values) do
    IO.iodata_to_binary([<<length(values)>> | Enum.map(values, &[<<byte_size(&1)>>, &1])])
  end

  defp info_values(values) do
    IO.iodata_to_binary([<<length(values)>> | Enum.map(values, &<<&1::32-big>>)])
  end

  defp put_byte(bin, index, byte) do
    before = binary_part(bin, 0, index)
    after_ = binary_part(bin, index + 1, byte_size(bin) - index - 1)
    before <> <<byte>> <> after_
  end
end
