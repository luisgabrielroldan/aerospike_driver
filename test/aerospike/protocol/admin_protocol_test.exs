defmodule Aerospike.Protocol.AdminProtocolTest do
  use ExUnit.Case, async: true

  alias Aerospike.Privilege
  alias Aerospike.Protocol.Admin
  alias Aerospike.Protocol.Message
  alias Aerospike.Role
  alias Aerospike.User

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

      assert {:error, :truncated_field_data} =
               Admin.decode_users_block(admin_body(0, 9, [<<5::32-big, 0, "abc">>]))

      assert {:error, :invalid_string_list_field} =
               Admin.decode_users_block(admin_body(0, 9, [admin_field(10, <<1, 1, ?a, 0>>)]))

      assert {:error, :invalid_connections_field} =
               Admin.decode_users_block(admin_body(0, 9, [admin_field(18, <<1, 2, 3>>)]))
    end

    test "returns result-code errors for non-success admin frames" do
      assert {:error, {:result_code, :invalid_user, 60}} =
               Admin.decode_users_block(admin_body(60, 9, []))
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
      assert {:error, {:unknown_privilege_code, 99}} =
               Admin.decode_roles_block(admin_body(0, 16, [admin_field(12, <<1, 99>>)]))

      assert {:error, :truncated_privilege_scope} =
               Admin.decode_roles_block(admin_body(0, 16, [admin_field(12, <<1, 11, 4, "tes">>)]))

      assert {:error, :invalid_read_quota_field} =
               Admin.decode_roles_block(admin_body(0, 16, [admin_field(14, <<1, 2>>)]))

      assert {:error, :invalid_write_quota_field} =
               Admin.decode_roles_block(admin_body(0, 16, [admin_field(15, <<1, 2>>)]))
    end
  end

  defp admin_body(result_code, command, fields) when is_list(fields) do
    IO.iodata_to_binary([<<0, result_code, command, length(fields), 0::96>>, fields])
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
