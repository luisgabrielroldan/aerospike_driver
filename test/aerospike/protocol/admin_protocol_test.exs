defmodule Aerospike.Protocol.AdminProtocolTest do
  use ExUnit.Case, async: true

  alias Aerospike.Privilege
  alias Aerospike.Protocol.Admin
  alias Aerospike.Protocol.Login
  alias Aerospike.Protocol.Message
  alias Aerospike.Role
  alias Aerospike.User

  describe "user and role command encoders" do
    test "encode fixed command ids and counted string fields" do
      assert_command(Admin.encode_create_user("ada", "secret", ["read", "write"]), 1, 3)
      assert_command(Admin.encode_drop_user("ada"), 2, 1)
      assert_command(Admin.encode_set_password("ada", "next"), 3, 2)
      assert_command(Admin.encode_change_password("ada", "old", "new"), 4, 3)
      assert_command(Admin.encode_grant_roles("ada", ["read"]), 5, 2)
      assert_command(Admin.encode_revoke_roles("ada", ["read"]), 6, 2)
      assert_command(Admin.encode_query_users(), 9, 0)
      assert_command(Admin.encode_query_users("ada"), 9, 1)
      assert_command(Admin.encode_drop_role("reader"), 11, 1)
      assert_command(Admin.encode_query_roles(), 16, 0)
      assert_command(Admin.encode_query_roles("reader"), 16, 1)
    end

    test "encodes PKI user creation as create-user with the no-password credential" do
      credential = Login.no_password_credential()
      frame = Admin.encode_create_pki_user("cert-user", credential, ["read", "write"])

      assert {1, 3, fields} = decode_command(frame)
      assert [{0, "cert-user"}, {1, ^credential}, {10, <<2, 4, "read", 5, "write">>}] = fields
    end

    test "encodes standalone whitelist mutation and omits an empty whitelist" do
      frame = Admin.encode_set_whitelist("analyst", ["10.0.0.1", "10.0.0.0/24"])

      assert {14, 2, fields} = decode_command(frame)
      assert [{11, "analyst"}, {13, "10.0.0.1,10.0.0.0/24"}] = fields

      empty_frame = Admin.encode_set_whitelist("analyst", [])

      assert {14, 1, [{11, "analyst"}]} = decode_command(empty_frame)
    end

    test "encodes standalone quotas and includes zero quota values" do
      frame = Admin.encode_set_quotas("analyst", 0, 120)

      assert {15, 3, fields} = decode_command(frame)
      assert [{11, "analyst"}, {14, <<0::32-big>>}, {15, <<120::32-big>>}] = fields
    end

    test "encode all supported privilege codes with optional role fields" do
      privileges = [
        %Privilege{code: :user_admin},
        %Privilege{code: :sys_admin},
        %Privilege{code: :data_admin},
        %Privilege{code: :udf_admin},
        %Privilege{code: :sindex_admin},
        %Privilege{code: :masking_admin},
        %Privilege{code: :read, namespace: "test"},
        %Privilege{code: :read_write, namespace: "test", set: "demo"},
        %Privilege{code: :read_write_udf, namespace: "test"},
        %Privilege{code: :write, namespace: "test"},
        %Privilege{code: :truncate, namespace: "test"},
        %Privilege{code: :read_masked, namespace: "test"},
        %Privilege{code: :write_masked, namespace: "test"}
      ]

      assert {:ok, frame} = Admin.encode_create_role("analyst", privileges, ["10.0.0.1"], 5, 6)
      assert_command(frame, 10, 5)

      assert {:ok, frame} = Admin.encode_grant_privileges("analyst", privileges)
      assert_command(frame, 12, 2)

      assert {:ok, frame} = Admin.encode_revoke_privileges("analyst", privileges)
      assert_command(frame, 13, 2)
    end
  end

  describe "encode_create_role/5" do
    test "omits optional fields when privileges, whitelist, and quotas are empty" do
      assert {:ok, frame} = Admin.encode_create_role("analyst", [], [], 0, 0)
      assert {:ok, {2, 2, body}} = Message.decode(frame)
      assert <<0, 0, 10, 1, _::binary>> = body
    end

    test "rejects invalid privilege scopes and unknown privilege codes" do
      assert {:error, {:invalid_privilege_scope, %Privilege{code: :user_admin}}} =
               Admin.encode_create_role(
                 "analyst",
                 [%Privilege{code: :user_admin, namespace: "test"}],
                 [],
                 0,
                 0
               )

      assert {:error, {:invalid_privilege_scope, %Privilege{code: :read_write}}} =
               Admin.encode_create_role(
                 "analyst",
                 [%Privilege{code: :read_write, set: "demo"}],
                 [],
                 0,
                 0
               )

      assert {:error, {:invalid_privilege_code, :bogus}} =
               Admin.encode_create_role(
                 "analyst",
                 [%Privilege{code: :bogus}],
                 [],
                 0,
                 0
               )
    end
  end

  describe "decode_admin_body/1" do
    test "reports unknown result codes and short bodies" do
      assert Admin.decode_admin_body(<<0, 240, 0>>) ==
               {:ok, %{result_code: :unknown_result_code, raw: 240}}

      assert Admin.decode_admin_body(<<0>>) == {:error, :short_body}
    end
  end

  describe "decode_users_block/1" do
    test "parses a partial users block without a query-end frame" do
      block =
        admin_body(
          0,
          9,
          [
            admin_field(0, "ada"),
            counted_string_field(10, ["read"]),
            info_field(16, [1, 2]),
            info_field(17, [3]),
            uint32_field(18, 9)
          ]
        )

      assert {:ok,
              %{
                done?: false,
                users: [
                  %User{
                    name: "ada",
                    roles: ["read"],
                    read_info: [1, 2],
                    write_info: [3],
                    connections_in_use: 9
                  }
                ]
              }} = Admin.decode_users_block(block)
    end

    test "surfaces truncated fields and malformed list payloads" do
      assert {:error, :truncated_record_header} = Admin.decode_users_block(<<0, 0, 0>>)
      assert {:ok, %{done?: false, users: []}} = Admin.decode_users_block(admin_body(0, 9, []))

      assert {:error, :truncated_field_header} =
               Admin.decode_users_block(admin_body_with_count(0, 9, 1, <<1, 2>>))

      assert {:error, :truncated_field_data} =
               Admin.decode_users_block(admin_body(0, 9, [<<5::32-big, 0, "abc">>]))

      assert {:error, :invalid_string_list_field} =
               Admin.decode_users_block(admin_body(0, 9, [admin_field(10, <<1, 1, ?a, 0>>)]))

      assert {:error, :invalid_connections_field} =
               Admin.decode_users_block(admin_body(0, 9, [admin_field(18, <<1, 2, 3>>)]))

      assert {:error, :invalid_info_field} =
               Admin.decode_users_block(admin_body(0, 9, [admin_field(16, <<1, 0, 0, 0, 1, 0>>)]))

      assert {:error, :truncated_info_field} =
               Admin.decode_users_block(admin_body(0, 9, [admin_field(17, <<1, 2>>)]))
    end

    test "returns result-code errors for non-success admin frames" do
      assert {:error, {:result_code, :invalid_user, 60}} =
               Admin.decode_users_block(admin_body(60, 9, []))

      assert {:error, {:result_code, {:unknown_result_code, 240}, 240}} =
               Admin.decode_users_block(admin_body(240, 9, []))
    end
  end

  describe "decode_roles_block/1" do
    test "parses scoped and global privileges and trims empty whitelist fragments" do
      block =
        admin_body(
          0,
          16,
          [
            admin_field(11, "analyst"),
            admin_field(12, <<2, 0, 11, 4, "test", 4, "demo">>),
            admin_field(13, "10.0.0.1,,10.0.0.0/24"),
            uint32_field(14, 25),
            uint32_field(15, 50)
          ]
        )

      done_block = admin_body(50, 16, [])

      assert {:ok,
              %{
                done?: true,
                roles: [
                  %Role{
                    name: "analyst",
                    privileges: [
                      %Privilege{code: :user_admin, namespace: nil, set: nil},
                      %Privilege{code: :read_write, namespace: "test", set: "demo"}
                    ],
                    whitelist: ["10.0.0.1", "10.0.0.0/24"],
                    read_quota: 25,
                    write_quota: 50
                  }
                ]
              }} = Admin.decode_roles_block(block <> done_block)
    end

    test "surfaces malformed privilege and quota payloads" do
      assert {:error, :truncated_record_header} = Admin.decode_roles_block(<<0, 0, 0>>)
      assert {:ok, %{done?: false, roles: []}} = Admin.decode_roles_block(admin_body(0, 16, []))

      assert {:error, :truncated_field_header} =
               Admin.decode_roles_block(admin_body_with_count(0, 16, 1, <<1, 2>>))

      assert {:error, {:unknown_privilege_code, 99}} =
               Admin.decode_roles_block(admin_body(0, 16, [admin_field(12, <<1, 99>>)]))

      assert {:error, :invalid_privileges_field} =
               Admin.decode_roles_block(admin_body(0, 16, [admin_field(12, <<0, 1>>)]))

      assert {:error, :truncated_privileges_field} =
               Admin.decode_roles_block(admin_body(0, 16, [admin_field(12, <<1>>)]))

      assert {:error, :truncated_privilege_scope} =
               Admin.decode_roles_block(admin_body(0, 16, [admin_field(12, <<1, 11, 4, "tes">>)]))

      assert {:error, :invalid_read_quota_field} =
               Admin.decode_roles_block(admin_body(0, 16, [admin_field(14, <<1, 2>>)]))

      assert {:error, :invalid_write_quota_field} =
               Admin.decode_roles_block(admin_body(0, 16, [admin_field(15, <<1, 2>>)]))

      assert {:error, {:result_code, :invalid_role, 70}} =
               Admin.decode_roles_block(admin_body(70, 16, []))
    end
  end

  defp assert_command(frame, command, field_count) do
    assert {:ok, {2, 2, <<0, 0, ^command, ^field_count, _::binary-size(12), _::binary>>}} =
             Message.decode(frame)
  end

  defp decode_command(frame) do
    assert {:ok, {2, 2, <<0, 0, command, field_count, _::binary-size(12), fields::binary>>}} =
             Message.decode(frame)

    {command, field_count, decode_fields(fields, field_count)}
  end

  defp decode_fields(<<>>, 0), do: []

  defp decode_fields(<<size::32-big, id, value::binary-size(size - 1), rest::binary>>, count)
       when count > 0 do
    [{id, value} | decode_fields(rest, count - 1)]
  end

  defp admin_body(result_code, command, fields) when is_list(fields) do
    IO.iodata_to_binary([<<0, result_code, command, length(fields), 0::96>>, fields])
  end

  defp admin_body_with_count(result_code, command, field_count, payload) do
    <<0, result_code, command, field_count, 0::96, payload::binary>>
  end

  defp admin_field(id, value) when is_integer(id) and is_binary(value) do
    <<byte_size(value) + 1::32-big, id::8, value::binary>>
  end

  defp counted_string_field(id, values) when is_integer(id) and is_list(values) do
    payload = [<<length(values)::8>>, Enum.map(values, &<<byte_size(&1)::8, &1::binary>>)]
    admin_field(id, IO.iodata_to_binary(payload))
  end

  defp info_field(id, values) when is_integer(id) and is_list(values) do
    payload = [<<length(values)::8>>, Enum.map(values, &<<&1::32-big>>)]
    admin_field(id, IO.iodata_to_binary(payload))
  end

  defp uint32_field(id, value) when is_integer(id) and is_integer(value) do
    admin_field(id, <<value::32-big>>)
  end
end
