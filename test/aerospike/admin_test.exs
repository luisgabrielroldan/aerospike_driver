defmodule Aerospike.Command.AdminTest do
  use ExUnit.Case, async: true

  alias Aerospike.Cluster.NodeSupervisor
  alias Aerospike.Cluster.PartitionMapWriter
  alias Aerospike.Cluster.TableOwner
  alias Aerospike.Cluster.Tender
  alias Aerospike.Error
  alias Aerospike.IndexTask
  alias Aerospike.Privilege
  alias Aerospike.Protocol.Message
  alias Aerospike.RegisterTask
  alias Aerospike.Role
  alias Aerospike.Test.ReplicasFixture
  alias Aerospike.Transport.Fake
  alias Aerospike.UDF
  alias Aerospike.User

  @namespace "test"

  setup do
    host = "10.0.0.1"
    port = 3_000
    name = :"admin_test_#{System.unique_integer([:positive, :monotonic])}"

    {:ok, fake} = Fake.start_link(nodes: [{"A1", host, port}])
    {:ok, owner} = TableOwner.start_link(name: name)
    tables = TableOwner.tables(owner)
    {:ok, writer} = PartitionMapWriter.start_link(name: name, tables: tables)
    {:ok, node_sup} = NodeSupervisor.start_link(name: name)

    {:ok, tender} =
      Tender.start_link(
        name: name,
        transport: Fake,
        connect_opts: [fake: fake],
        seeds: [{host, port}],
        namespaces: [@namespace],
        tables: tables,
        tend_trigger: :manual,
        node_supervisor: NodeSupervisor.sup_name(name),
        pool_size: 1
      )

    script_single_node_cluster(fake)
    :ok = Tender.tend_now(tender)

    on_exit(fn ->
      stop_quietly(tender)
      stop_quietly(node_sup)
      stop_quietly(writer)
      stop_quietly(owner)
      stop_quietly(fake)
    end)

    {:ok, conn: tender, fake: fake}
  end

  test "create_index/4 returns a task and the task polls to completion", %{
    conn: conn,
    fake: fake
  } do
    set = "admin_idx_#{System.unique_integer([:positive, :monotonic])}"
    index_name = "age_idx_#{System.unique_integer([:positive, :monotonic])}"

    Fake.script_info(fake, "A1", ["build"], %{"build" => "8.1.0.0"})

    Fake.script_info(
      fake,
      "A1",
      ["sindex-create:namespace=test;set=#{set};indexname=#{index_name};bin=age;type=NUMERIC"],
      %{
        "sindex-create:namespace=test;set=#{set};indexname=#{index_name};bin=age;type=NUMERIC" =>
          "OK"
      }
    )

    Fake.script_info(fake, "A1", ["build"], %{"build" => "8.1.0.0"})

    Fake.script_info(fake, "A1", ["sindex-stat:namespace=test;indexname=#{index_name}"], %{
      "sindex-stat:namespace=test;indexname=#{index_name}" => "load_pct=47;state=RW"
    })

    Fake.script_info(fake, "A1", ["build"], %{"build" => "8.1.0.0"})

    Fake.script_info(fake, "A1", ["sindex-stat:namespace=test;indexname=#{index_name}"], %{
      "sindex-stat:namespace=test;indexname=#{index_name}" => "load_pct=100;state=RW"
    })

    Fake.script_info(fake, "A1", ["build"], %{"build" => "8.1.0.0"})

    Fake.script_info(fake, "A1", ["sindex-delete:namespace=test;indexname=#{index_name}"], %{
      "sindex-delete:namespace=test;indexname=#{index_name}" => "OK"
    })

    assert {:ok, %IndexTask{} = task} =
             Aerospike.create_index(conn, @namespace, set,
               bin: "age",
               name: index_name,
               type: :numeric
             )

    assert task.conn == conn
    assert task.namespace == @namespace
    assert task.index_name == index_name
    assert :ok = IndexTask.wait(task, timeout: 2_000, poll_interval: 10)
    assert :ok = Aerospike.drop_index(conn, @namespace, index_name)
  end

  test "index task status treats blank and missing load_pct replies as complete", %{
    conn: conn,
    fake: fake
  } do
    blank_task = %IndexTask{conn: conn, namespace: @namespace, index_name: "blank_idx"}
    missing_task = %IndexTask{conn: conn, namespace: @namespace, index_name: "missing_pct_idx"}

    Fake.script_info(fake, "A1", ["build"], %{"build" => "8.1.0.0"})

    Fake.script_info(fake, "A1", ["sindex-stat:namespace=test;indexname=blank_idx"], %{
      "sindex-stat:namespace=test;indexname=blank_idx" => ""
    })

    Fake.script_info(fake, "A1", ["build"], %{"build" => "8.1.0.0"})

    Fake.script_info(fake, "A1", ["sindex-stat:namespace=test;indexname=missing_pct_idx"], %{
      "sindex-stat:namespace=test;indexname=missing_pct_idx" => "state=RW"
    })

    assert {:ok, :complete} = IndexTask.status(blank_task)
    assert {:ok, :complete} = IndexTask.status(missing_task)
  end

  test "index task status surfaces in-progress and admin lookup errors", %{conn: conn, fake: fake} do
    progress_task = %IndexTask{conn: conn, namespace: @namespace, index_name: "progress_idx"}
    missing_task = %IndexTask{conn: conn, namespace: @namespace, index_name: "missing_idx"}

    Fake.script_info(fake, "A1", ["build"], %{"build" => "8.1.0.0"})

    Fake.script_info(fake, "A1", ["sindex-stat:namespace=test;indexname=progress_idx"], %{
      "sindex-stat:namespace=test;indexname=progress_idx" => "load_pct=47;state=RW"
    })

    Fake.script_info(fake, "A1", ["build"], %{"build" => "8.1.0.0"})
    Fake.script_info(fake, "A1", ["sindex-stat:namespace=test;indexname=missing_idx"], %{})

    assert {:ok, :in_progress} = IndexTask.status(progress_task)
    assert {:ok, :complete} = IndexTask.status(missing_task)
  end

  test "admin helpers reject invalid checkout policy values before issuing info commands", %{
    conn: conn
  } do
    assert {:error, %Error{code: :invalid_argument, message: message}} =
             Aerospike.create_index(conn, @namespace, "users",
               bin: "age",
               name: "age_idx",
               type: :numeric,
               pool_checkout_timeout: -1
             )

    assert message =~ "pool_checkout_timeout must be a non-negative integer"
  end

  test "info/3 returns one info reply from an active node", %{conn: conn, fake: fake} do
    Fake.script_info(fake, "A1", ["namespaces"], %{"namespaces" => "test"})

    assert {:ok, "test"} = Aerospike.info(conn, "namespaces")
  end

  test "list_udfs/2 parses inventory entries and skips blank fragments", %{conn: conn, fake: fake} do
    Fake.script_info(fake, "A1", ["udf-list"], %{
      "udf-list" =>
        "filename=alpha.lua,hash=abc123,type=LUA; ;filename=beta.lua,hash=def456,type=LUA;"
    })

    assert {:ok,
            [
              %UDF{filename: "alpha.lua", hash: "abc123", language: "LUA"},
              %UDF{filename: "beta.lua", hash: "def456", language: "LUA"}
            ]} = Aerospike.Command.Admin.list_udfs(conn, [])
  end

  test "register_udf/4 uploads inline source and returns a register task", %{
    conn: conn,
    fake: fake
  } do
    source = "function echo(rec, arg) return arg end"
    server_name = "echo.lua"
    encoded = Base.encode64(source)

    command =
      "udf-put:filename=#{server_name};content=#{encoded};content-len=#{byte_size(encoded)};udf-type=LUA;"

    Fake.script_info(fake, "A1", [command], %{command => "OK"})

    assert {:ok, %RegisterTask{conn: ^conn, package_name: ^server_name}} =
             Aerospike.Command.Admin.register_udf(conn, source, server_name, [])
  end

  test "remove_udf/3 treats missing packages as an idempotent success", %{conn: conn, fake: fake} do
    command = "udf-remove:filename=missing.lua;"
    Fake.script_info(fake, "A1", [command], %{command => "error=file not found"})

    assert :ok = Aerospike.Command.Admin.remove_udf(conn, "missing.lua", [])
  end

  test "truncate/3 builds the namespace command and appends lut when provided", %{
    conn: conn,
    fake: fake
  } do
    before = DateTime.from_unix!(1_700_000_000, :second)
    command = "truncate-namespace:namespace=test;lut=#{DateTime.to_unix(before, :nanosecond)}"

    Fake.script_info(fake, "A1", [command], %{command => "OK"})

    assert :ok = Aerospike.Command.Admin.truncate(conn, @namespace, before: before)
  end

  test "truncate/4 builds the set command and returns :ok", %{conn: conn, fake: fake} do
    command = "truncate:namespace=test;set=users"
    Fake.script_info(fake, "A1", [command], %{command => "OK"})

    assert :ok = Aerospike.Command.Admin.truncate(conn, @namespace, "users", [])
  end

  test "query_users/2 decodes streamed admin frames into user structs", %{conn: conn, fake: fake} do
    user_block =
      admin_frame(
        0,
        9,
        [
          admin_field(0, "ada"),
          counted_string_field(10, ["read", "read-write"]),
          info_field(16, [1, 2]),
          info_field(17, [3]),
          uint32_field(18, 4)
        ]
      )

    done_block = admin_frame(50, 9, [])
    Fake.script_command_stream(fake, "A1", {:ok, user_block <> done_block})

    assert {:ok,
            [
              %User{
                name: "ada",
                roles: ["read", "read-write"],
                read_info: [1, 2],
                write_info: [3],
                connections_in_use: 4
              }
            ]} = Aerospike.Command.Admin.query_users(conn, [])
  end

  test "create_role sends scoped privileges, whitelist, and quotas", %{conn: conn, fake: fake} do
    privilege = %Privilege{code: :read_write, namespace: "test", set: "demo"}
    ok_body = <<0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0>>

    Fake.script_command(fake, "A1", {:ok, ok_body})

    assert :ok =
             Aerospike.Command.Admin.create_role(
               conn,
               "analyst",
               [privilege],
               ["10.0.0.0/24"],
               25,
               50,
               []
             )
  end

  test "query_roles reads streamed admin frames into role structs", %{conn: conn, fake: fake} do
    role_block =
      admin_frame(
        0,
        16,
        [
          admin_field(11, "analyst"),
          admin_field(12, <<1, 11, 4, "test", 4, "demo">>),
          admin_field(13, "10.0.0.1,10.0.0.0/24"),
          uint32_field(14, 25),
          uint32_field(15, 50)
        ]
      )

    done_block = admin_frame(50, 16, [])
    Fake.script_command_stream(fake, "A1", {:ok, role_block <> done_block})

    assert {:ok,
            [
              %Role{
                name: "analyst",
                privileges: [%Privilege{code: :read_write, namespace: "test", set: "demo"}],
                whitelist: ["10.0.0.1", "10.0.0.0/24"],
                read_quota: 25,
                write_quota: 50
              }
            ]} = Aerospike.Command.Admin.query_roles(conn, [])
  end

  test "query_role returns nil when the server streams no matching records", %{
    conn: conn,
    fake: fake
  } do
    done_block = admin_frame(50, 16, [])
    Fake.script_command_stream(fake, "A1", {:ok, done_block})

    assert {:ok, nil} = Aerospike.Command.Admin.query_role(conn, "missing-role", [])
  end

  test "query_roles translates malformed privilege payloads into protocol errors", %{
    conn: conn,
    fake: fake
  } do
    malformed_role =
      admin_frame(
        0,
        16,
        [
          admin_field(11, "analyst"),
          admin_field(12, <<1, 11, 4, "tes">>)
        ]
      )

    done_block = admin_frame(50, 16, [])
    Fake.script_command_stream(fake, "A1", {:ok, malformed_role <> done_block})

    assert {:error, %Error{code: :server_error, message: message}} =
             Aerospike.Command.Admin.query_roles(conn, [])

    assert message == "invalid admin response: :truncated_privilege_scope"
  end

  test "change_password/4 uses self-change semantics and rotates tender credentials", %{
    conn: conn,
    fake: fake
  } do
    Fake.script_command(fake, "A1", {:ok, <<0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0>>})
    :ok = Tender.rotate_auth_credentials(conn, "alice", "old-secret")

    assert :ok = Aerospike.Command.Admin.change_password(conn, "alice", "new-secret", [])
    assert %{user: "alice", password: "new-secret"} = Tender.auth_credentials(conn)
  end

  defp script_single_node_cluster(fake) do
    Fake.script_info(fake, "A1", ["node", "features"], %{"node" => "A1", "features" => ""})

    Fake.script_info(
      fake,
      "A1",
      ["partition-generation", "cluster-stable", "peers-generation"],
      %{
        "partition-generation" => "1",
        "cluster-stable" => "deadbeef",
        "peers-generation" => "1"
      }
    )

    Fake.script_info(fake, "A1", ["peers-clear-std"], %{"peers-clear-std" => "0,3000,[]"})

    Fake.script_info(fake, "A1", ["replicas"], %{
      "replicas" => ReplicasFixture.all_master(@namespace, 1)
    })
  end

  defp stop_quietly(pid) when is_pid(pid) do
    if Process.alive?(pid) do
      ref = Process.monitor(pid)
      Process.exit(pid, :shutdown)

      receive do
        {:DOWN, ^ref, _, _, _} -> :ok
      after
        1_000 -> :ok
      end
    end
  end

  defp admin_frame(result_code, command, fields) when is_list(fields) do
    body = IO.iodata_to_binary([<<0, result_code, command, length(fields), 0::96>>, fields])
    Message.encode(2, 2, body)
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
