defmodule Aerospike.AdminTest do
  use ExUnit.Case, async: false

  alias Aerospike.Admin
  alias Aerospike.Admin.PasswordHash
  alias Aerospike.NodePool
  alias Aerospike.Policy
  alias Aerospike.Privilege
  alias Aerospike.Role
  alias Aerospike.Tables
  alias Aerospike.Test.MockTcpServer
  alias Aerospike.User

  defp start_ets(name) do
    :ets.new(Tables.nodes(name), [:set, :public, :named_table, read_concurrency: true])
    :ets.new(Tables.meta(name), [:set, :public, :named_table])

    on_exit(fn ->
      for t <- [Tables.nodes(name), Tables.meta(name)] do
        try do
          :ets.delete(t)
        catch
          :error, :badarg -> :ok
        end
      end
    end)
  end

  describe "nodes/1" do
    setup do
      name = :"admin_utest_#{:erlang.unique_integer([:positive])}"
      start_ets(name)
      {:ok, name: name}
    end

    test "returns cluster_not_ready when cluster has not finished tending", %{name: name} do
      assert {:error, %{code: :cluster_not_ready}} = Admin.nodes(name)
    end

    test "returns empty list when no nodes are registered", %{name: name} do
      :ets.insert(Tables.meta(name), {Tables.ready_key(), true})
      assert {:ok, []} = Admin.nodes(name)
    end

    test "returns node entries from ETS", %{name: name} do
      :ets.insert(Tables.meta(name), {Tables.ready_key(), true})

      :ets.insert(Tables.nodes(name), {"node1", %{host: "127.0.0.1", port: 3000, active: true}})
      :ets.insert(Tables.nodes(name), {"node2", %{host: "127.0.0.2", port: 3001, active: true}})

      assert {:ok, nodes} = Admin.nodes(name)
      names = Enum.map(nodes, & &1.name) |> Enum.sort()
      assert names == ["node1", "node2"]

      node1 = Enum.find(nodes, &(&1.name == "node1"))
      assert node1.host == "127.0.0.1"
      assert node1.port == 3000
    end
  end

  describe "node_names/1" do
    setup do
      name = :"admin_utest_#{:erlang.unique_integer([:positive])}"
      start_ets(name)
      {:ok, name: name}
    end

    test "returns cluster_not_ready before cluster is ready", %{name: name} do
      assert {:error, %{code: :cluster_not_ready}} = Admin.node_names(name)
    end

    test "returns empty list when no nodes are registered", %{name: name} do
      :ets.insert(Tables.meta(name), {Tables.ready_key(), true})
      assert {:ok, []} = Admin.node_names(name)
    end

    test "returns sorted list of node names", %{name: name} do
      :ets.insert(Tables.meta(name), {Tables.ready_key(), true})
      :ets.insert(Tables.nodes(name), {"node-b", %{host: "h", port: 1, active: true}})
      :ets.insert(Tables.nodes(name), {"node-a", %{host: "h", port: 2, active: true}})

      assert {:ok, names} = Admin.node_names(name)
      assert Enum.sort(names) == ["node-a", "node-b"]
    end
  end

  describe "Policy.validate_info/1" do
    test "accepts empty opts" do
      assert {:ok, []} = Policy.validate_info([])
    end

    test "accepts timeout and pool_checkout_timeout" do
      assert {:ok, opts} = Policy.validate_info(timeout: 1_000, pool_checkout_timeout: 2_000)
      assert opts[:timeout] == 1_000
      assert opts[:pool_checkout_timeout] == 2_000
    end

    test "rejects non-integer timeout" do
      assert {:error, %NimbleOptions.ValidationError{}} = Policy.validate_info(timeout: "bad")
    end

    test "rejects unknown keys" do
      assert {:error, %NimbleOptions.ValidationError{}} = Policy.validate_info(unknown: true)
    end
  end

  describe "Aerospike facade info validation" do
    test "rejects bad opts and returns parameter_error" do
      assert {:error, %Aerospike.Error{code: :parameter_error}} =
               Aerospike.info(:not_a_real_conn, "namespaces", timeout: -1)
    end

    test "rejects unknown opts" do
      assert {:error, %Aerospike.Error{code: :parameter_error}} =
               Aerospike.info(:not_a_real_conn, "namespaces", unknown_opt: true)
    end
  end

  describe "list_udfs/2" do
    setup do
      name = :"admin_udf_list_#{:erlang.unique_integer([:positive])}"
      start_ets(name)
      {:ok, name: name}
    end

    test "returns cluster_not_ready when connection ETS tables do not exist" do
      missing_name = :"admin_missing_#{System.unique_integer([:positive, :monotonic])}"

      assert {:error, %Aerospike.Error{code: :cluster_not_ready}} =
               Admin.list_udfs(missing_name, [])

      assert {:error, %Aerospike.Error{code: :cluster_not_ready}} =
               Aerospike.list_udfs(missing_name)
    end

    test "returns an empty list for an empty udf-list response", %{name: name} do
      {:ok, pool_pid, server} =
        start_pool_with_server(name, fn client ->
          {:ok, _header, body} = MockTcpServer.recv_message(client)
          assert body == "udf-list\n"
          MockTcpServer.send_info_response(client, "udf-list\t\n")
        end)

      register_node(name, pool_pid, 3_020)

      assert {:ok, []} = Admin.list_udfs(name, [])
      Task.await(server)
    end

    test "parses multiple udf inventory entries and skips blank fragments", %{name: name} do
      {:ok, pool_pid, server} =
        start_pool_with_server(name, fn client ->
          {:ok, _header, body} = MockTcpServer.recv_message(client)
          assert body == "udf-list\n"

          MockTcpServer.send_info_response(
            client,
            "udf-list\tfilename=alpha.lua,hash=abc123,type=LUA; ;filename=beta.lua,hash=def456,type=LUA;\n"
          )
        end)

      register_node(name, pool_pid, 3_021)

      assert {:ok,
              [
                %Aerospike.UDF{filename: "alpha.lua", hash: "abc123", language: "LUA"},
                %Aerospike.UDF{filename: "beta.lua", hash: "def456", language: "LUA"}
              ]} = Admin.list_udfs(name, [])

      Task.await(server)
    end

    test "returns server_error for malformed udf inventory fragments", %{name: name} do
      {:ok, pool_pid, server} =
        start_pool_with_server(name, fn client ->
          {:ok, _header, body} = MockTcpServer.recv_message(client)
          assert body == "udf-list\n"
          MockTcpServer.send_info_response(client, "udf-list\tfilename=broken.lua,hash=abc123;\n")
        end)

      register_node(name, pool_pid, 3_022)

      assert {:error, %Aerospike.Error{code: :server_error, message: message}} =
               Admin.list_udfs(name, [])

      assert message ==
               "invalid udf-list entry (missing type): \"filename=broken.lua,hash=abc123\""

      Task.await(server)
    end
  end

  describe "PasswordHash.hash/1" do
    test "matches the Go client for known inputs" do
      assert PasswordHash.hash("secret") ==
               "$2a$10$7EqJtq98hPqEX7fNZaFWoOu7NRZ0i7m5NbgPBEsHTCoJzZDEKQRb."

      assert PasswordHash.hash("nopassword") ==
               "$2a$10$7EqJtq98hPqEX7fNZaFWoOePA2sZy..tlOF99W2N0g5KZ3dLaw8WO"

      assert PasswordHash.hash("päss") ==
               "$2a$10$7EqJtq98hPqEX7fNZaFWoOZ.W4PCgs4.Y7GyBsIkFn1qcfWAvlrUC"
    end
  end

  describe "security admin execution" do
    setup do
      name = :"admin_security_#{System.unique_integer([:positive, :monotonic])}"
      start_ets(name)
      {:ok, name: name}
    end

    test "create_user sends the hashed password credential", %{name: name} do
      {:ok, pool_pid, server} =
        start_pool_with_server(name, fn client ->
          {:ok, _header, body} = MockTcpServer.recv_message(client)
          assert command_id(body) == 1
          fields = decode_fields(body)
          assert Map.fetch!(fields, 0) == "ada"
          assert Map.fetch!(fields, 1) == PasswordHash.hash("secret")
          MockTcpServer.send_admin_response(client, :binary.copy(<<0>>, 16))
        end)

      register_node(name, pool_pid, 3_000)

      assert :ok = Admin.create_user(name, "ada", "secret", ["read-write"], [])
      Task.await(server)
    end

    test "create_pki_user sends the no-password credential", %{name: name} do
      {:ok, pool_pid, server} =
        start_pool_with_server(name, fn client ->
          {:ok, _header, body} = MockTcpServer.recv_message(client)
          assert command_id(body) == 1
          fields = decode_fields(body)
          assert Map.fetch!(fields, 0) == "cert-user"
          assert Map.fetch!(fields, 1) == PasswordHash.no_password_credential()
          assert Map.fetch!(fields, 10) == counted_strings(["read-write"])
          MockTcpServer.send_admin_response(client, :binary.copy(<<0>>, 16))
        end)

      register_node(name, pool_pid, 3_010)

      assert :ok = Admin.create_pki_user(name, "cert-user", ["read-write"], [])
      Task.await(server)
    end

    test "change_password uses the self-service command when the auth user matches", %{name: name} do
      old_credential = PasswordHash.hash("oldsecret")

      {:ok, pool_pid, server} =
        start_pool_with_server(
          name,
          fn client ->
            {:ok, _login_header, login_body} = MockTcpServer.recv_message(client)
            assert command_id(login_body) == 20
            MockTcpServer.send_admin_response(client, :binary.copy(<<0>>, 16))

            {:ok, _header, body} = MockTcpServer.recv_message(client)
            assert command_id(body) == 4
            fields = decode_fields(body)
            assert Map.fetch!(fields, 0) == "alice"
            assert Map.fetch!(fields, 2) == old_credential
            assert Map.fetch!(fields, 1) == PasswordHash.hash("newsecret")
            MockTcpServer.send_admin_response(client, :binary.copy(<<0>>, 16))
          end,
          user: "alice",
          credential: old_credential
        )

      register_node(name, pool_pid, 3_001)
      :ets.insert(Tables.meta(name), {:auth_opts, [user: "alice", credential: old_credential]})

      assert :ok = Admin.change_password(name, "alice", "newsecret", [])

      assert [{:auth_opts, auth_opts}] = :ets.lookup(Tables.meta(name), :auth_opts)
      assert Keyword.fetch!(auth_opts, :credential) == PasswordHash.hash("newsecret")
      Task.await(server)
    end

    test "change_password uses the admin set-password command for non-self changes", %{name: name} do
      caller_credential = PasswordHash.hash("admin-secret")

      {:ok, pool_pid, server} =
        start_pool_with_server(
          name,
          fn client ->
            {:ok, _login_header, login_body} = MockTcpServer.recv_message(client)
            assert command_id(login_body) == 20
            MockTcpServer.send_admin_response(client, :binary.copy(<<0>>, 16))

            {:ok, _header, body} = MockTcpServer.recv_message(client)
            assert command_id(body) == 3
            fields = decode_fields(body)
            assert Map.fetch!(fields, 0) == "bob"
            assert Map.fetch!(fields, 1) == PasswordHash.hash("newsecret")
            refute Map.has_key?(fields, 2)
            MockTcpServer.send_admin_response(client, :binary.copy(<<0>>, 16))
          end,
          user: "alice",
          credential: caller_credential
        )

      register_node(name, pool_pid, 3_005)
      :ets.insert(Tables.meta(name), {:auth_opts, [user: "alice", credential: caller_credential]})

      assert :ok = Admin.change_password(name, "bob", "newsecret", [])

      assert [{:auth_opts, auth_opts}] = :ets.lookup(Tables.meta(name), :auth_opts)
      assert Keyword.fetch!(auth_opts, :credential) == caller_credential
      Task.await(server)
    end

    test "change_password does not rotate auth state when self-change fails", %{name: name} do
      old_credential = PasswordHash.hash("oldsecret")

      {:ok, pool_pid, server} =
        start_pool_with_server(
          name,
          fn client ->
            {:ok, _login_header, login_body} = MockTcpServer.recv_message(client)
            assert command_id(login_body) == 20
            MockTcpServer.send_admin_response(client, :binary.copy(<<0>>, 16))

            {:ok, _header, body} = MockTcpServer.recv_message(client)
            assert command_id(body) == 4
            fields = decode_fields(body)
            assert Map.fetch!(fields, 0) == "alice"
            assert Map.fetch!(fields, 2) == old_credential
            assert Map.fetch!(fields, 1) == PasswordHash.hash("newsecret")
            MockTcpServer.send_admin_response(client, admin_record(64, []))
          end,
          user: "alice",
          credential: old_credential
        )

      register_node(name, pool_pid, 3_006)
      :ets.insert(Tables.meta(name), {:auth_opts, [user: "alice", credential: old_credential]})

      assert {:error, %{code: :forbidden_password}} =
               Admin.change_password(name, "alice", "newsecret", [])

      assert [{:auth_opts, auth_opts}] = :ets.lookup(Tables.meta(name), :auth_opts)
      assert Keyword.fetch!(auth_opts, :credential) == old_credential
      Task.await(server)
    end

    test "create_user translates admin result-code failures", %{name: name} do
      {:ok, pool_pid, server} =
        start_pool_with_server(name, fn client ->
          {:ok, _header, body} = MockTcpServer.recv_message(client)
          assert command_id(body) == 1
          MockTcpServer.send_admin_response(client, admin_record(60, []))
        end)

      register_node(name, pool_pid, 3_007)

      assert {:error, %{code: :invalid_user}} =
               Admin.create_user(name, "ada", "secret", ["read-write"], [])

      Task.await(server)
    end

    test "query_users reads streamed admin responses until query_end", %{name: name} do
      {:ok, pool_pid, server} =
        start_pool_with_server(name, fn client ->
          {:ok, _header, body} = MockTcpServer.recv_message(client)
          assert command_id(body) == 9

          user_body =
            admin_record(
              0,
              [
                field(0, "ada"),
                field(10, counted_strings(["read-write", "sys-admin"]))
              ]
            )

          MockTcpServer.send_admin_response(client, user_body)
          MockTcpServer.send_admin_response(client, admin_record(50, []))
        end)

      register_node(name, pool_pid, 3_002)

      assert {:ok, [%User{name: "ada", roles: ["read-write", "sys-admin"]}]} =
               Admin.query_users(name, [])

      Task.await(server)
    end

    test "query_users translates streamed admin result-code failures", %{name: name} do
      {:ok, pool_pid, server} =
        start_pool_with_server(name, fn client ->
          {:ok, _header, body} = MockTcpServer.recv_message(client)
          assert command_id(body) == 9
          MockTcpServer.send_admin_response(client, admin_record(60, []))
        end)

      register_node(name, pool_pid, 3_008)

      assert {:error, %{code: :invalid_user}} = Admin.query_users(name, [])
      Task.await(server)
    end

    test "query_user returns nil when the server streams no matching records", %{name: name} do
      {:ok, pool_pid, server} =
        start_pool_with_server(name, fn client ->
          {:ok, _header, body} = MockTcpServer.recv_message(client)
          assert command_id(body) == 9
          MockTcpServer.send_admin_response(client, admin_record(50, []))
        end)

      register_node(name, pool_pid, 3_011)

      assert {:ok, nil} = Admin.query_user(name, "missing-user", [])
      Task.await(server)
    end

    test "create_role sends scoped privileges, whitelist, and quotas", %{name: name} do
      privilege = %Privilege{code: :read_write, namespace: "test", set: "demo"}

      {:ok, pool_pid, server} =
        start_pool_with_server(name, fn client ->
          {:ok, _header, body} = MockTcpServer.recv_message(client)
          assert command_id(body) == 10
          fields = decode_fields(body)
          assert Map.fetch!(fields, 11) == "analyst"
          assert Map.fetch!(fields, 12) == <<1, 11, 4, "test", 4, "demo">>
          assert Map.fetch!(fields, 13) == "10.0.0.0/24"
          assert Map.fetch!(fields, 14) == <<25::32-big>>
          assert Map.fetch!(fields, 15) == <<50::32-big>>
          MockTcpServer.send_admin_response(client, :binary.copy(<<0>>, 16))
        end)

      register_node(name, pool_pid, 3_003)

      assert :ok =
               Admin.create_role(
                 name,
                 "analyst",
                 [privilege],
                 whitelist: ["10.0.0.0/24"],
                 read_quota: 25,
                 write_quota: 50
               )

      Task.await(server)
    end

    test "query_roles reads streamed admin responses until query_end", %{name: name} do
      {:ok, pool_pid, server} =
        start_pool_with_server(name, fn client ->
          {:ok, _header, body} = MockTcpServer.recv_message(client)
          assert command_id(body) == 16

          role_body =
            admin_record(
              0,
              [
                field(11, "analyst"),
                field(12, <<1, 11, 4, "test", 4, "demo">>),
                field(13, "10.0.0.1,10.0.0.0/24"),
                field(14, <<25::32-big>>),
                field(15, <<50::32-big>>)
              ]
            )

          MockTcpServer.send_admin_response(client, role_body)
          MockTcpServer.send_admin_response(client, admin_record(50, []))
        end)

      register_node(name, pool_pid, 3_004)

      assert {:ok,
              [
                %Role{
                  name: "analyst",
                  privileges: [%Privilege{code: :read_write, namespace: "test", set: "demo"}],
                  whitelist: ["10.0.0.1", "10.0.0.0/24"],
                  read_quota: 25,
                  write_quota: 50
                }
              ]} = Admin.query_roles(name, [])

      Task.await(server)
    end

    test "query_roles translates malformed streamed admin payloads into protocol errors", %{
      name: name
    } do
      {:ok, pool_pid, server} =
        start_pool_with_server(name, fn client ->
          {:ok, _header, body} = MockTcpServer.recv_message(client)
          assert command_id(body) == 16

          malformed_role_body =
            admin_record(
              0,
              [
                field(11, "analyst"),
                field(12, <<1, 11, 4, "tes">>)
              ]
            )

          MockTcpServer.send_admin_response(client, malformed_role_body)
          MockTcpServer.send_admin_response(client, admin_record(50, []))
        end)

      register_node(name, pool_pid, 3_009)

      assert {:error, %{code: :server_error, message: message}} = Admin.query_roles(name, [])
      assert message == "invalid admin response: :truncated_privilege_scope"
      Task.await(server)
    end

    test "query_role returns nil when the server streams no matching records", %{name: name} do
      {:ok, pool_pid, server} =
        start_pool_with_server(name, fn client ->
          {:ok, _header, body} = MockTcpServer.recv_message(client)
          assert command_id(body) == 16
          MockTcpServer.send_admin_response(client, admin_record(50, []))
        end)

      register_node(name, pool_pid, 3_012)

      assert {:ok, nil} = Admin.query_role(name, "missing-role", [])
      Task.await(server)
    end

    test "query_role preserves default whitelist and quotas when optional fields are absent", %{
      name: name
    } do
      {:ok, pool_pid, server} =
        start_pool_with_server(name, fn client ->
          {:ok, _header, body} = MockTcpServer.recv_message(client)
          assert command_id(body) == 16

          role_body =
            admin_record(
              0,
              [
                field(11, "observer")
              ]
            )

          MockTcpServer.send_admin_response(client, role_body)
          MockTcpServer.send_admin_response(client, admin_record(50, []))
        end)

      register_node(name, pool_pid, 3_013)

      assert {:ok,
              %Role{
                name: "observer",
                privileges: [],
                whitelist: [],
                read_quota: 0,
                write_quota: 0
              }} = Admin.query_role(name, "observer", [])

      Task.await(server)
    end
  end

  defp start_pool_with_server(name, handler, auth_opts \\ []) do
    {:ok, listen_socket, port} = MockTcpServer.start()

    server =
      Task.async(fn ->
        MockTcpServer.accept_once(listen_socket, handler)
      end)

    pool_pid =
      start_supervised!(
        Supervisor.child_spec(
          {NimblePool,
           [
             worker:
               {NodePool,
                connect_opts: [
                  host: "127.0.0.1",
                  port: port,
                  timeout: 30_000,
                  recv_timeout: 30_000
                ],
                auth_opts: auth_opts},
             pool_size: 1
           ]},
          id: {NimblePool, name, port}
        )
      )

    {:ok, pool_pid, server}
  end

  defp register_node(name, pool_pid, port) do
    :ets.insert(Tables.meta(name), {Tables.ready_key(), true})

    :ets.insert(
      Tables.nodes(name),
      {"node1", %{pool_pid: pool_pid, host: "127.0.0.1", port: port, active: true}}
    )
  end

  defp command_id(<<_status::8, _result_code::8, command::8, _field_count::8, _rest::binary>>),
    do: command

  defp decode_fields(<<_admin::binary-size(16), rest::binary>>) do
    decode_fields(rest, %{})
  end

  defp decode_fields(<<>>, acc), do: acc

  defp decode_fields(<<len::32-big, id::8, value::binary-size(len - 1), rest::binary>>, acc) do
    decode_fields(rest, Map.put(acc, id, value))
  end

  defp admin_record(result_code, fields) do
    <<0, result_code, 0, length(fields)>> <>
      :binary.copy(<<0>>, 12) <> IO.iodata_to_binary(fields)
  end

  defp field(id, value) when is_binary(value) do
    <<byte_size(value) + 1::32-big, id::8, value::binary>>
  end

  defp counted_strings(values) when is_list(values) do
    [<<length(values)>> | Enum.map(values, fn value -> [<<byte_size(value)>>, value] end)]
    |> IO.iodata_to_binary()
  end
end
