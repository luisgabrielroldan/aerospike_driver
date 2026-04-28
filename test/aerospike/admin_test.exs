defmodule Aerospike.Command.AdminTest do
  use ExUnit.Case, async: true

  alias Aerospike.Cluster.NodeSupervisor
  alias Aerospike.Cluster.PartitionMapWriter
  alias Aerospike.Cluster.TableOwner
  alias Aerospike.Cluster.Tender
  alias Aerospike.Command.Admin
  alias Aerospike.Error
  alias Aerospike.Exp
  alias Aerospike.IndexTask
  alias Aerospike.Privilege
  alias Aerospike.Protocol.Login
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

  test "create_expression_index/5 encodes an expression source and returns a task", %{
    conn: conn,
    fake: fake
  } do
    set = "admin_expr_idx_#{System.unique_integer([:positive, :monotonic])}"
    index_name = "age_expr_idx_#{System.unique_integer([:positive, :monotonic])}"

    command =
      "sindex-create:namespace=test;set=#{set};indexname=#{index_name};exp=k1ECo2FnZQ==;type=NUMERIC"

    Fake.script_info(fake, "A1", ["build"], %{"build" => "8.1.0.0"})
    Fake.script_info(fake, "A1", [command], %{command => "OK"})

    assert {:ok, %IndexTask{} = task} =
             Aerospike.create_expression_index(conn, @namespace, set, Exp.int_bin("age"),
               name: index_name,
               type: :numeric
             )

    assert task.conn == conn
    assert task.namespace == @namespace
    assert task.index_name == index_name
  end

  test "create_index/4 encodes a geo2dsphere bin index", %{conn: conn, fake: fake} do
    set = "admin_geo_idx_#{System.unique_integer([:positive, :monotonic])}"
    index_name = "geo_idx_#{System.unique_integer([:positive, :monotonic])}"

    command =
      "sindex-create:namespace=test;set=#{set};indexname=#{index_name};bin=loc;type=GEO2DSPHERE"

    Fake.script_info(fake, "A1", ["build"], %{"build" => "8.1.0.0"})
    Fake.script_info(fake, "A1", [command], %{command => "OK"})

    assert {:ok, %IndexTask{} = task} =
             Aerospike.create_index(conn, @namespace, set,
               bin: "loc",
               name: index_name,
               type: :geo2dsphere
             )

    assert task.conn == conn
    assert task.namespace == @namespace
    assert task.index_name == index_name
  end

  test "create_expression_index/5 encodes collection type without a bin source", %{
    conn: conn,
    fake: fake
  } do
    set = "admin_expr_list_idx_#{System.unique_integer([:positive, :monotonic])}"
    index_name = "age_expr_list_idx_#{System.unique_integer([:positive, :monotonic])}"

    command =
      "sindex-create:namespace=test;set=#{set};indexname=#{index_name};exp=k1ECo2FnZQ==;indextype=LIST;type=NUMERIC"

    Fake.script_info(fake, "A1", ["build"], %{"build" => "8.1.0.0"})
    Fake.script_info(fake, "A1", [command], %{command => "OK"})

    assert {:ok, %IndexTask{namespace: @namespace, index_name: ^index_name}} =
             Admin.create_expression_index(
               conn,
               @namespace,
               set,
               Exp.int_bin("age"),
               name: index_name,
               type: :numeric,
               collection: :list
             )
  end

  test "create_expression_index/5 rejects unsupported server versions before create", %{
    conn: conn,
    fake: fake
  } do
    Fake.script_info(fake, "A1", ["build"], %{"build" => "8.0.0.0"})

    assert {:error, %Error{code: :parameter_error, message: message}} =
             Admin.create_expression_index(
               conn,
               @namespace,
               "users",
               Exp.int_bin("age"),
               name: "age_expr_idx",
               type: :numeric
             )

    assert message ==
             "expression-backed secondary indexes require Aerospike server 8.1.0 or newer"
  end

  test "create_expression_index/5 rejects an empty expression before create", %{
    conn: conn,
    fake: fake
  } do
    Fake.script_info(fake, "A1", ["build"], %{"build" => "8.1.0.0"})

    assert {:error, %Error{code: :invalid_argument, message: message}} =
             Admin.create_expression_index(
               conn,
               @namespace,
               "users",
               Exp.from_wire(""),
               name: "age_expr_idx",
               type: :numeric
             )

    assert message =~ "%Aerospike.Exp{}"
    assert message =~ "non-empty wire bytes"
  end

  test "create_expression_index/5 wraps non-OK server responses", %{
    conn: conn,
    fake: fake
  } do
    set = "admin_expr_fail_idx_#{System.unique_integer([:positive, :monotonic])}"
    index_name = "age_expr_fail_idx_#{System.unique_integer([:positive, :monotonic])}"

    command =
      "sindex-create:namespace=test;set=#{set};indexname=#{index_name};exp=k1ECo2FnZQ==;type=NUMERIC"

    Fake.script_info(fake, "A1", ["build"], %{"build" => "8.1.0.0"})
    Fake.script_info(fake, "A1", [command], %{command => "FAIL: duplicate index"})

    assert {:error, %Error{code: :server_error, message: message}} =
             Admin.create_expression_index(
               conn,
               @namespace,
               set,
               Exp.int_bin("age"),
               name: index_name,
               type: :numeric
             )

    assert message == ~s(unexpected info response: "FAIL: duplicate index")
  end

  test "set_xdr_filter/4 encodes an expression payload", %{conn: conn, fake: fake} do
    command = "xdr-set-filter:dc=dc-west;namespace=test;exp=kwGTUQKjYmluVQ=="

    Fake.script_info(fake, "A1", [command], %{command => "OK"})

    assert :ok =
             Aerospike.set_xdr_filter(
               conn,
               "dc-west",
               @namespace,
               Exp.eq(Exp.int_bin("bin"), Exp.int(85))
             )
  end

  test "set_xdr_filter/4 uses exp=null when clearing a filter", %{conn: conn, fake: fake} do
    command = "xdr-set-filter:dc=dc-west;namespace=test;exp=null"

    Fake.script_info(fake, "A1", [command], %{command => "OK"})

    assert :ok = Aerospike.set_xdr_filter(conn, "dc-west", @namespace, nil)
  end

  test "set_xdr_filter/4 validates identifiers and filters before command execution", %{
    conn: conn
  } do
    assert {:error, %Error{code: :invalid_argument, message: dc_message}} =
             Aerospike.set_xdr_filter(conn, "", @namespace, nil)

    assert dc_message =~ "datacenter must be a non-empty string"

    assert {:error, %Error{code: :invalid_argument, message: namespace_message}} =
             Aerospike.set_xdr_filter(conn, "dc-west", "test;users", nil)

    assert namespace_message =~ "namespace cannot contain info-command delimiters"

    assert {:error, %Error{code: :invalid_argument, message: filter_message}} =
             Aerospike.set_xdr_filter(conn, "dc-west", @namespace, :invalid)

    assert filter_message =~ "XDR filters require nil or a %Aerospike.Exp{}"
  end

  test "set_xdr_filter/4 rejects empty expression wires before command execution", %{conn: conn} do
    assert {:error, %Error{code: :invalid_argument, message: message}} =
             Aerospike.set_xdr_filter(conn, "dc-west", @namespace, Exp.from_wire(""))

    assert message =~ "XDR filters require nil or a %Aerospike.Exp{}"
    assert message =~ "non-empty wire bytes"
  end

  test "set_xdr_filter/4 wraps non-OK info responses", %{conn: conn, fake: fake} do
    command = "xdr-set-filter:dc=dc-west;namespace=test;exp=null"

    Fake.script_info(fake, "A1", [command], %{command => "ERROR::unsupported feature"})

    assert {:error, %Error{code: :server_error, message: message}} =
             Aerospike.set_xdr_filter(conn, "dc-west", @namespace, nil)

    assert message == ~s(unexpected info response: "ERROR::unsupported feature")
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

  test "index task waits for every active node to report complete" do
    name = :"admin_index_task_cluster_#{System.unique_integer([:positive, :monotonic])}"

    {:ok, fake} =
      Fake.start_link(nodes: [{"A1", "10.0.0.1", 3000}, {"B1", "10.0.0.2", 3000}])

    {:ok, owner} = TableOwner.start_link(name: name)
    tables = TableOwner.tables(owner)
    {:ok, writer} = PartitionMapWriter.start_link(name: name, tables: tables)
    {:ok, node_sup} = NodeSupervisor.start_link(name: name)

    {:ok, tender} =
      Tender.start_link(
        name: name,
        transport: Fake,
        connect_opts: [fake: fake],
        seeds: [{"10.0.0.1", 3000}, {"10.0.0.2", 3000}],
        namespaces: [@namespace],
        tables: tables,
        tend_trigger: :manual,
        node_supervisor: NodeSupervisor.sup_name(name),
        pool_size: 1
      )

    on_exit(fn ->
      stop_quietly(tender)
      stop_quietly(node_sup)
      stop_quietly(writer)
      stop_quietly(owner)
      stop_quietly(fake)
    end)

    script_two_node_cluster(fake)
    :ok = Tender.tend_now(tender)

    task = %IndexTask{conn: tender, namespace: @namespace, index_name: "multi_idx"}

    Fake.script_info(fake, "A1", ["build"], %{"build" => "8.1.0.0"})

    Fake.script_info(fake, "A1", ["sindex-stat:namespace=test;indexname=multi_idx"], %{
      "sindex-stat:namespace=test;indexname=multi_idx" => "load_pct=100;state=RW"
    })

    Fake.script_info(fake, "B1", ["build"], %{"build" => "8.1.0.0"})

    Fake.script_info(fake, "B1", ["sindex-stat:namespace=test;indexname=multi_idx"], %{
      "sindex-stat:namespace=test;indexname=multi_idx" => "load_pct=47;state=RW"
    })

    assert {:ok, :in_progress} = IndexTask.status(task)

    for node <- ["A1", "B1"] do
      Fake.script_info(fake, node, ["build"], %{"build" => "8.1.0.0"})

      Fake.script_info(fake, node, ["sindex-stat:namespace=test;indexname=multi_idx"], %{
        "sindex-stat:namespace=test;indexname=multi_idx" => "load_pct=100;state=RW"
      })
    end

    assert :ok = IndexTask.wait(task, timeout: 100, poll_interval: 0)
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
            ]} = Admin.list_udfs(conn, [])
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
             Admin.register_udf(conn, source, server_name, [])
  end

  test "remove_udf/3 treats missing packages as an idempotent success", %{conn: conn, fake: fake} do
    command = "udf-remove:filename=missing.lua;"
    Fake.script_info(fake, "A1", [command], %{command => "error=file not found"})

    assert :ok = Admin.remove_udf(conn, "missing.lua", [])
  end

  test "truncate/3 builds the namespace command and appends lut when provided", %{
    conn: conn,
    fake: fake
  } do
    before = DateTime.from_unix!(1_700_000_000, :second)
    command = "truncate-namespace:namespace=test;lut=#{DateTime.to_unix(before, :nanosecond)}"

    Fake.script_info(fake, "A1", [command], %{command => "OK"})

    assert :ok = Admin.truncate(conn, @namespace, before: before)
  end

  test "truncate/4 builds the set command and returns :ok", %{conn: conn, fake: fake} do
    command = "truncate:namespace=test;set=users"
    Fake.script_info(fake, "A1", [command], %{command => "OK"})

    assert :ok = Admin.truncate(conn, @namespace, "users", [])
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
            ]} = Admin.query_users(conn, [])
  end

  test "create_pki_user/4 rejects unsupported server versions before create", %{
    conn: conn,
    fake: fake
  } do
    Fake.script_info(fake, "A1", ["build"], %{"build" => "8.0.0.0"})

    assert {:error, %Error{code: :parameter_error, message: message}} =
             Admin.create_pki_user(conn, "cert-user", ["read"], [])

    assert message == "PKI user creation requires Aerospike server 8.1.0 or newer"
    assert Fake.last_command_request(fake, "A1") == nil
  end

  test "create_pki_user/4 checks server version and sends no-password credential", %{
    conn: conn,
    fake: fake
  } do
    ok_body = <<0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0>>

    Fake.script_info(fake, "A1", ["build"], %{"build" => "8.1.0.0"})
    Fake.script_command(fake, "A1", {:ok, ok_body})

    assert :ok = Admin.create_pki_user(conn, "cert-user", ["read"], [])

    credential = Login.no_password_credential()
    request = Fake.last_command_request(fake, "A1")

    assert {:ok, {2, 2, <<0, 0, 1, 3, _::binary-size(12), fields::binary>>}} =
             Message.decode(request)

    assert [
             {0, "cert-user"},
             {1, ^credential},
             {10, <<1, 4, "read">>}
           ] = decode_admin_fields(fields, 3)
  end

  test "create_role sends scoped privileges, whitelist, and quotas", %{conn: conn, fake: fake} do
    privilege = %Privilege{code: :read_write, namespace: "test", set: "demo"}
    ok_body = <<0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0>>

    Fake.script_command(fake, "A1", {:ok, ok_body})

    assert :ok =
             Admin.create_role(
               conn,
               "analyst",
               [privilege],
               ["10.0.0.0/24"],
               25,
               50,
               []
             )
  end

  test "set_whitelist sends role and omits empty whitelist", %{conn: conn, fake: fake} do
    ok_body = <<0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0>>

    Fake.script_command(fake, "A1", {:ok, ok_body})
    assert :ok = Admin.set_whitelist(conn, "analyst", ["10.0.0.1", "10.0.0.0/24"], [])

    request = Fake.last_command_request(fake, "A1")

    assert {:ok, {2, 2, <<0, 0, 14, 2, _::binary-size(12), fields::binary>>}} =
             Message.decode(request)

    assert [
             {11, "analyst"},
             {13, "10.0.0.1,10.0.0.0/24"}
           ] = decode_admin_fields(fields, 2)

    Fake.script_command(fake, "A1", {:ok, ok_body})
    assert :ok = Admin.set_whitelist(conn, "analyst", [], [])

    clear_request = Fake.last_command_request(fake, "A1")

    assert {:ok, {2, 2, <<0, 0, 14, 1, _::binary-size(12), clear_fields::binary>>}} =
             Message.decode(clear_request)

    assert [{11, "analyst"}] = decode_admin_fields(clear_fields, 1)
  end

  test "set_quotas sends role and explicit zero quota fields", %{conn: conn, fake: fake} do
    ok_body = <<0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0>>

    Fake.script_command(fake, "A1", {:ok, ok_body})

    assert :ok = Admin.set_quotas(conn, "analyst", 0, 25, [])

    request = Fake.last_command_request(fake, "A1")

    assert {:ok, {2, 2, <<0, 0, 15, 3, _::binary-size(12), fields::binary>>}} =
             Message.decode(request)

    assert [
             {11, "analyst"},
             {14, <<0::32-big>>},
             {15, <<25::32-big>>}
           ] = decode_admin_fields(fields, 3)
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
            ]} = Admin.query_roles(conn, [])
  end

  test "query_role returns nil when the server streams no matching records", %{
    conn: conn,
    fake: fake
  } do
    done_block = admin_frame(50, 16, [])
    Fake.script_command_stream(fake, "A1", {:ok, done_block})

    assert {:ok, nil} = Admin.query_role(conn, "missing-role", [])
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
             Admin.query_roles(conn, [])

    assert message == "invalid admin response: :truncated_privilege_scope"
  end

  test "change_password/4 uses self-change semantics and rotates tender credentials", %{
    conn: conn,
    fake: fake
  } do
    Fake.script_command(fake, "A1", {:ok, <<0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0>>})
    :ok = Tender.rotate_auth_credentials(conn, "alice", "old-secret")

    assert :ok = Admin.change_password(conn, "alice", "new-secret", [])
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

  defp script_two_node_cluster(fake) do
    for node <- ["A1", "B1"] do
      Fake.script_info(fake, node, ["node", "features"], %{"node" => node, "features" => ""})

      Fake.script_info(
        fake,
        node,
        ["partition-generation", "cluster-stable", "peers-generation"],
        %{
          "partition-generation" => "1",
          "cluster-stable" => "deadbeef",
          "peers-generation" => "1"
        }
      )
    end

    for node <- ["A1", "B1"] do
      Fake.script_info(fake, node, ["peers-clear-std"], %{
        "peers-clear-std" => "0,3000,[[::1]:3000,[::2]:3000]"
      })

      Fake.script_info(fake, node, ["replicas"], %{
        "replicas" => ReplicasFixture.all_master(@namespace, 2)
      })
    end
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

  defp decode_admin_fields(<<>>, 0), do: []

  defp decode_admin_fields(
         <<size::32-big, id, value::binary-size(size - 1), rest::binary>>,
         count
       )
       when count > 0 do
    [{id, value} | decode_admin_fields(rest, count - 1)]
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
