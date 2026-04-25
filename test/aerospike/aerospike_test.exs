defmodule Aerospike.PublicApiTest do
  use ExUnit.Case, async: false

  alias Aerospike.Cluster
  alias Aerospike.Cluster.Tender
  alias Aerospike.Cursor
  alias Aerospike.ExecuteTask
  alias Aerospike.Filter
  alias Aerospike.Key
  alias Aerospike.PartitionFilter
  alias Aerospike.Privilege
  alias Aerospike.Protocol.AsmMsg
  alias Aerospike.Protocol.AsmMsg.Field
  alias Aerospike.Protocol.AsmMsg.Operation
  alias Aerospike.Protocol.AsmMsg.Value
  alias Aerospike.Protocol.Message
  alias Aerospike.Query
  alias Aerospike.RegisterTask
  alias Aerospike.Role
  alias Aerospike.Scan
  alias Aerospike.Test.ReplicasFixture
  alias Aerospike.Transport.Fake
  alias Aerospike.Txn
  alias Aerospike.UDF
  alias Aerospike.User

  @namespace "test"
  @aggregate_sum_source """
  local function reducer(left, right)
    return left + right
  end

  function sum_values(stream)
    return stream : reduce(reducer)
  end
  """

  setup context do
    name = :"aerospike_test_#{:erlang.phash2(context.test)}"

    {:ok, fake} = Fake.start_link(nodes: [{"A1", "10.0.0.1", 3000}, {"B1", "10.0.0.2", 3000}])

    {:ok, sup} =
      Aerospike.start_link(
        name: name,
        transport: Fake,
        hosts: ["10.0.0.1:3000", "10.0.0.2:3000"],
        namespaces: [@namespace],
        connect_opts: [fake: fake],
        tend_trigger: :manual,
        pool_size: 1
      )

    script_two_node_cluster(fake)
    :ok = Tender.tend_now(name)
    assert Cluster.ready?(name)

    on_exit(fn ->
      stop_quietly(sup)
      stop_quietly(fake)
    end)

    {:ok, conn: name, conn_name: name, fake: fake, sup: sup}
  end

  test "cluster read-side helpers expose readiness and active nodes", %{conn_name: conn_name} do
    assert Cluster.ready?(conn_name)
    assert Enum.sort(Cluster.active_nodes(conn_name)) == ["A1", "B1"]
    assert Cluster.active_node?(conn_name, "A1")
    refute Cluster.active_node?(conn_name, "missing")
  end

  test "operator helpers expose one-node info replies and active node metadata", %{
    conn: conn,
    fake: fake
  } do
    Fake.script_info(fake, "A1", ["statistics"], %{"statistics" => "objects=42"})

    Fake.script_info(fake, "A1", ["truncate-namespace:namespace=test"], %{
      "truncate-namespace:namespace=test" => "OK"
    })

    assert {:ok, "objects=42"} = Aerospike.info(conn, "statistics")
    assert :ok = Aerospike.truncate(conn, "test")

    assert {:ok, ["A1", "B1"]} = Aerospike.node_names(conn)

    assert {:ok, nodes} = Aerospike.nodes(conn)

    assert Enum.sort(nodes) == [
             %{name: "A1", host: "10.0.0.1", port: 3000},
             %{name: "B1", host: "10.0.0.2", port: 3000}
           ]
  end

  test "info_node targets a named active node and preserves stale node errors", %{
    conn: conn,
    fake: fake
  } do
    Fake.script_info(fake, "B1", ["statistics"], %{"statistics" => "objects=99"})

    assert {:ok, "objects=99"} = Aerospike.info_node(conn, "B1", "statistics")

    assert {:error, %Aerospike.Error{code: :invalid_node, node: "missing"}} =
             Aerospike.info_node(conn, "missing", "statistics")
  end

  test "scan and query node targeting does not expose duplicate root helpers" do
    absent_helpers = [
      {:query_stream_node, 4},
      {:query_stream_node!, 4},
      {:query_all_node, 4},
      {:query_all_node!, 4},
      {:query_count_node, 4},
      {:query_count_node!, 4},
      {:query_page_node, 4},
      {:query_page_node!, 4},
      {:query_execute_node, 5},
      {:query_execute_node!, 5},
      {:query_udf_node, 7},
      {:query_udf_node!, 7},
      {:scan_stream_node, 4},
      {:scan_stream_node!, 4},
      {:scan_all_node, 4},
      {:scan_all_node!, 4},
      {:scan_count_node, 4},
      {:scan_count_node!, 4},
      {:scan_page_node, 4}
    ]

    for {name, arity} <- absent_helpers do
      refute function_exported?(Aerospike, name, arity), "#{name}/#{arity} should not be public"
    end
  end

  test "metrics and warm-up helpers stay explicit and opt-in", %{conn: conn} do
    refute Aerospike.metrics_enabled?(conn)

    assert :ok = Aerospike.enable_metrics(conn, reset: true)
    assert Aerospike.metrics_enabled?(conn)

    assert %{
             metrics_enabled: true,
             cluster: %{config: %{pool_size: 1}}
           } = Aerospike.stats(conn)

    assert {:ok,
            %{
              status: :ok,
              requested_per_node: 1,
              total_requested: 2,
              total_warmed: 2,
              nodes_total: 2,
              nodes_ok: 2,
              nodes_partial: 0,
              nodes_error: 0,
              nodes: %{
                "A1" => %{host: "10.0.0.1", port: 3000, requested: 1, warmed: 1, status: :ok},
                "B1" => %{host: "10.0.0.2", port: 3000, requested: 1, warmed: 1, status: :ok}
              }
            }} = Aerospike.warm_up(conn)

    assert :ok = Aerospike.disable_metrics(conn)
    refute Aerospike.metrics_enabled?(conn)

    assert {:error, %Aerospike.Error{code: :invalid_argument, message: reset_message}} =
             Aerospike.enable_metrics(conn, reset: :now)

    assert reset_message =~ ":reset must be a boolean"

    assert {:error, %Aerospike.Error{code: :invalid_argument, message: warm_up_message}} =
             Aerospike.warm_up(conn, count: -1)

    assert warm_up_message =~ ":count must be a non-negative integer"
  end

  test "root lifecycle and key helpers expose the supported public boundary", %{
    conn_name: conn_name,
    sup: sup
  } do
    assert %{
             id: {Aerospike.Cluster.Supervisor, ^conn_name},
             start: {Aerospike.Cluster.Supervisor, :start_link, [[name: ^conn_name]]},
             type: :supervisor
           } = Aerospike.child_spec(name: conn_name)

    digest = :crypto.hash(:ripemd160, "digest-only")

    assert %Key{namespace: "test", set: "users", user_key: "user:42"} =
             Aerospike.key("test", "users", "user:42")

    assert %Key{namespace: "test", set: "users", user_key: nil, digest: ^digest} =
             Aerospike.key_digest("test", "users", digest)

    ref = Process.monitor(sup)

    assert :ok = Aerospike.close(conn_name)

    assert_receive {:DOWN, ^ref, :process, ^sup, _}, 1_000
  end

  test "single-record facade helpers normalize tuple keys and reject invalid tuples", %{
    conn: conn
  } do
    assert {:error, %Aerospike.Error{code: :invalid_argument, message: bins_message}} =
             Aerospike.get(conn, {"test", "users", "user:1"}, ["name"])

    assert bins_message =~ "supports only :all and :header read modes"

    assert {:error, %Aerospike.Error{code: :invalid_argument, message: key_message}} =
             Aerospike.exists(conn, {"test", :users, "user:1"})

    assert key_message =~ "set must be a string"
  end

  test "write convenience facade helpers normalize tuple keys and atom bin names", %{
    conn: conn,
    fake: fake
  } do
    for node <- ["A1", "B1"] do
      Fake.script_command(fake, node, {:ok, scripted_reply_body(0, 2, 15)})
      Fake.script_command(fake, node, {:ok, scripted_reply_body(0, 3, 15)})
      Fake.script_command(fake, node, {:ok, scripted_reply_body(0, 4, 15)})
    end

    assert {:ok, %{generation: 2, ttl: 15}} =
             Aerospike.add(conn, {"test", "users", "user:1"}, %{count: 2})

    assert {:ok, %{generation: 3, ttl: 15}} =
             Aerospike.append(conn, {"test", "users", "user:1"}, %{name: "x"})

    assert {:ok, %{generation: 4, ttl: 15}} =
             Aerospike.prepend(conn, {"test", "users", "user:1"}, %{name: "y"})
  end

  test "header-only facade helper returns metadata with empty bins and normalizes tuple keys", %{
    conn: conn,
    fake: fake
  } do
    Fake.script_command(fake, "A1", {:ok, scripted_reply_body(0, 4, 60)})
    Fake.script_command(fake, "B1", {:ok, scripted_reply_body(0, 4, 60)})

    assert {:ok, %Aerospike.Record{generation: 4, ttl: 60, bins: %{}}} =
             Aerospike.get_header(conn, {"test", "users", "user:1"})
  end

  test "single-record UDF facade normalizes tuple keys and returns typed results", %{
    conn: conn,
    fake: fake
  } do
    Fake.script_command(fake, "A1", {:ok, udf_reply_body("SUCCESS", "done")})
    Fake.script_command(fake, "B1", {:ok, udf_reply_body("SUCCESS", "done")})

    assert {:ok, "done"} =
             Aerospike.apply_udf(conn, {"test", "users", "user:1"}, "pkg", "echo", ["done"])

    Fake.script_command(fake, "A1", {:ok, udf_reply_body("FAILURE", "boom", 100)})
    Fake.script_command(fake, "B1", {:ok, udf_reply_body("FAILURE", "boom", 100)})

    assert {:error, %Aerospike.Error{code: :udf_bad_response, message: "boom"}} =
             Aerospike.apply_udf(conn, {"test", "users", "user:1"}, "pkg", "explode", [])
  end

  test "public UDF lifecycle helpers return metadata and pollable task handles", %{
    conn: conn,
    fake: fake
  } do
    source = "function echo(rec, arg) return arg end"
    server_name = "lifecycle.lua"
    encoded = Base.encode64(source)

    register_command =
      "udf-put:filename=#{server_name};content=#{encoded};content-len=#{byte_size(encoded)};udf-type=LUA;"

    Fake.script_info(fake, "A1", [register_command], %{register_command => "OK"})

    Fake.script_info(fake, "A1", ["udf-list"], %{
      "udf-list" => "filename=lifecycle.lua,hash=abc123,type=LUA;"
    })

    Fake.script_info(fake, "A1", ["udf-remove:filename=lifecycle.lua;"], %{
      "udf-remove:filename=lifecycle.lua;" => "OK"
    })

    assert {:ok, %RegisterTask{package_name: ^server_name}} =
             Aerospike.register_udf(conn, source, server_name)

    assert {:ok, [%UDF{filename: ^server_name, hash: "abc123", language: "LUA"}]} =
             Aerospike.list_udfs(conn)

    assert :ok = Aerospike.remove_udf(conn, server_name)
  end

  test "truncate helpers expose namespace and set variants and reject invalid opts", %{
    conn: conn,
    fake: fake
  } do
    before = DateTime.from_unix!(1_700_000_000, :second)

    namespace_command =
      "truncate-namespace:namespace=test;lut=#{DateTime.to_unix(before, :nanosecond)}"

    set_command = "truncate:namespace=test;set=users"

    Fake.script_info(fake, "A1", [namespace_command], %{namespace_command => "OK"})
    Fake.script_info(fake, "A1", [set_command], %{set_command => "OK"})

    assert :ok = Aerospike.truncate(conn, "test", before: before)
    assert :ok = Aerospike.truncate(conn, "test", "users")

    assert {:error, %Aerospike.Error{code: :invalid_argument, message: before_message}} =
             Aerospike.truncate(conn, "test", before: :yesterday)

    assert before_message =~ ":before must be a DateTime"

    assert {:error, %Aerospike.Error{code: :invalid_argument, message: opt_message}} =
             Aerospike.truncate(conn, "test", "users", bad_opt: true)

    assert opt_message =~
             "Aerospike.truncate/4 supports only :before and :pool_checkout_timeout options"
  end

  test "security admin helpers expose the narrow public user surface", %{conn: conn, fake: fake} do
    ok_body = <<0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0>>
    Fake.script_command(fake, "A1", {:ok, ok_body})
    Fake.script_command(fake, "A1", {:ok, ok_body})
    Fake.script_command(fake, "A1", {:ok, ok_body})
    Fake.script_command(fake, "A1", {:ok, ok_body})
    Fake.script_command(fake, "A1", {:ok, ok_body})
    Fake.script_command(fake, "A1", {:ok, ok_body})
    Fake.script_command(fake, "A1", {:ok, ok_body})

    user_query =
      admin_frame(
        0,
        9,
        [admin_field(0, "ada"), counted_string_field(10, ["read"]), uint32_field(18, 1)]
      ) <> admin_frame(50, 9, [])

    Fake.script_command_stream(fake, "A1", {:ok, user_query})
    Fake.script_command_stream(fake, "A1", {:ok, user_query})

    assert :ok = Aerospike.create_user(conn, "ada", "secret", ["read"])
    Fake.script_info(fake, "A1", ["build"], %{"build" => "8.1.0.0"})
    assert :ok = Aerospike.create_pki_user(conn, "cert-ada", ["read"])
    assert :ok = Aerospike.grant_roles(conn, "ada", ["read-write"])
    assert :ok = Aerospike.revoke_roles(conn, "ada", ["read"])

    :ok = Tender.rotate_auth_credentials(conn, "ada", "secret")
    assert :ok = Aerospike.change_password(conn, "ada", "rotated")
    assert %{user: "ada", password: "rotated"} = Tender.auth_credentials(conn)

    assert {:ok, %User{name: "ada", roles: ["read"], connections_in_use: 1}} =
             Aerospike.query_user(conn, "ada")

    assert {:ok, [%User{name: "ada"}]} = Aerospike.query_users(conn)
    assert :ok = Aerospike.drop_user(conn, "ada")

    assert {:error, %Aerospike.Error{code: :invalid_argument, message: message}} =
             Aerospike.create_user(conn, "ada", "secret", [:read])

    assert message =~ "roles must be a list of strings"

    assert {:error, %Aerospike.Error{code: :invalid_argument, message: pki_roles_message}} =
             Aerospike.create_pki_user(conn, "cert-ada", [:read])

    assert pki_roles_message =~ "roles must be a list of strings"

    assert {:error, %Aerospike.Error{code: :invalid_argument, message: opt_message}} =
             Aerospike.query_users(conn, bogus: true)

    assert opt_message =~
             "security admin commands support only :timeout and :pool_checkout_timeout"

    assert {:error, %Aerospike.Error{code: :invalid_argument, message: pki_opt_message}} =
             Aerospike.create_pki_user(conn, "cert-ada", ["read"], bogus: true)

    assert pki_opt_message =~
             "security admin commands support only :timeout and :pool_checkout_timeout"
  end

  test "security admin role helpers expose the narrow public role surface", %{
    conn: conn,
    fake: fake
  } do
    ok_body = <<0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0>>
    privilege = %Privilege{code: :read, namespace: "test", set: "reports"}
    extra_privilege = %Privilege{code: :read_write, namespace: "test", set: "reports_rw"}

    Fake.script_command(fake, "A1", {:ok, ok_body})
    Fake.script_command(fake, "A1", {:ok, ok_body})
    Fake.script_command(fake, "A1", {:ok, ok_body})
    Fake.script_command(fake, "A1", {:ok, ok_body})
    Fake.script_command(fake, "A1", {:ok, ok_body})
    Fake.script_command(fake, "A1", {:ok, ok_body})

    role_query =
      admin_frame(
        0,
        16,
        [
          admin_field(11, "analyst"),
          admin_field(12, <<2, 10, 4, "test", 7, "reports", 11, 4, "test", 10, "reports_rw">>),
          admin_field(13, "127.0.0.1"),
          uint32_field(14, 100),
          uint32_field(15, 200)
        ]
      ) <> admin_frame(50, 16, [])

    Fake.script_command_stream(fake, "A1", {:ok, role_query})
    Fake.script_command_stream(fake, "A1", {:ok, role_query})

    assert :ok =
             Aerospike.create_role(conn, "analyst", [privilege],
               whitelist: ["127.0.0.1"],
               read_quota: 100,
               write_quota: 200
             )

    assert :ok = Aerospike.set_whitelist(conn, "analyst", [])
    assert :ok = Aerospike.set_quotas(conn, "analyst", 0, 0)
    assert :ok = Aerospike.grant_privileges(conn, "analyst", [extra_privilege])
    assert :ok = Aerospike.revoke_privileges(conn, "analyst", [extra_privilege])

    assert {:ok,
            %Role{
              name: "analyst",
              privileges: [^privilege, ^extra_privilege],
              whitelist: ["127.0.0.1"],
              read_quota: 100,
              write_quota: 200
            }} = Aerospike.query_role(conn, "analyst")

    assert {:ok, [%Role{name: "analyst"}]} = Aerospike.query_roles(conn)
    assert :ok = Aerospike.drop_role(conn, "analyst")

    assert {:error, %Aerospike.Error{code: :invalid_argument, message: message}} =
             Aerospike.create_role(conn, "analyst", [:read])

    assert message =~ "privileges must be a list of %Aerospike.Privilege{} structs"

    assert {:error, %Aerospike.Error{code: :invalid_argument, message: opt_message}} =
             Aerospike.create_role(conn, "analyst", [privilege], bogus: true)

    assert opt_message =~
             "Aerospike.create_role/4 supports only :whitelist, :read_quota, :write_quota, :timeout, and :pool_checkout_timeout options"

    assert {:error, %Aerospike.Error{code: :invalid_argument, message: whitelist_message}} =
             Aerospike.set_whitelist(conn, "analyst", [:local])

    assert whitelist_message =~ ":whitelist must be a list of strings"

    assert {:error, %Aerospike.Error{code: :invalid_argument, message: quota_message}} =
             Aerospike.set_quotas(conn, "analyst", -1, 0)

    assert quota_message =~ ":read_quota must be a non-negative integer"

    assert {:error, %Aerospike.Error{code: :invalid_argument, message: write_quota_message}} =
             Aerospike.set_quotas(conn, "analyst", 0, "fast")

    assert write_quota_message =~ ":write_quota must be a non-negative integer"

    assert {:error, %Aerospike.Error{code: :invalid_argument, message: role_opt_message}} =
             Aerospike.set_whitelist(conn, "analyst", [], bogus: true)

    assert role_opt_message =~
             "security admin commands support only :timeout and :pool_checkout_timeout"

    assert {:error, %Aerospike.Error{code: :invalid_argument, message: quota_opt_message}} =
             Aerospike.set_quotas(conn, "analyst", 0, 0, bogus: true)

    assert quota_opt_message =~
             "security admin commands support only :timeout and :pool_checkout_timeout"
  end

  test "public scan and query wrappers return records, counts, pages, and task handles", %{
    conn: conn,
    fake: fake
  } do
    scan = Scan.new(@namespace, "scan_ops")

    Fake.script_stream(fake, "A1", {:ok, [frame("stream-A1"), last_frame()]})
    Fake.script_stream(fake, "B1", {:ok, [frame("stream-B1"), last_frame()]})

    assert {:ok, stream} = Aerospike.scan_stream(conn, scan)
    assert Enum.sort(Enum.map(stream, & &1.bins["payload"])) == ["stream-A1", "stream-B1"]

    Fake.script_stream(fake, "A1", {:ok, [frame("all-A1"), last_frame()]})
    Fake.script_stream(fake, "B1", {:ok, [frame("all-B1"), last_frame()]})

    assert {:ok, records} = Aerospike.scan_all(conn, scan)
    assert Enum.sort(Enum.map(records, & &1.bins["payload"])) == ["all-A1", "all-B1"]

    Fake.script_stream(fake, "A1", {:ok, [frame("count-A1"), frame("count-A2"), last_frame()]})
    Fake.script_stream(fake, "B1", {:ok, [frame("count-B1"), last_frame()]})

    assert 3 = Aerospike.scan_count!(conn, scan)

    Fake.script_stream(fake, "A1", {:ok, [frame("node-A1"), last_frame()]})
    assert [%{bins: %{"payload" => "node-A1"}}] = Aerospike.scan_all!(conn, scan, node: "A1")

    Fake.script_stream(fake, "A1", {:ok, [frame("node-count"), last_frame()]})
    assert 1 = Aerospike.scan_count!(conn, scan, node: "A1")

    query =
      Query.new(@namespace, "scan_ops")
      |> Query.where(Filter.range("payload", 0, 9))
      |> Query.max_records(1)

    Fake.script_stream(fake, "A1", {:ok, [frame("q-count-A1"), last_frame()]})

    Fake.script_stream(
      fake,
      "B1",
      {:ok, [frame("q-count-B1"), frame("q-count-B2"), last_frame()]}
    )

    assert 3 = Aerospike.query_count!(conn, query)

    Fake.script_stream(
      fake,
      "A1",
      {:ok, [frame("page-1"), partition_done_frame("page-1"), last_frame()]}
    )

    Fake.script_stream(fake, "B1", {:ok, [last_frame()]})

    assert %{records: [%{bins: %{"payload" => "page-1"}}], done?: false, cursor: %Cursor{}} =
             Aerospike.query_page!(conn, query)

    Fake.script_stream(fake, "A1", {:ok, [frame("page-node-1"), last_frame()]})

    node_query = Query.partition_filter(query, PartitionFilter.by_id(0))

    assert %{records: [%{bins: %{"payload" => "page-node-1"}}], done?: false, cursor: %Cursor{}} =
             Aerospike.query_page!(conn, node_query, node: "A1")

    Fake.script_stream(fake, "A1", {:ok, [frame("page-node-count"), last_frame()]})
    assert 1 = Aerospike.query_count!(conn, node_query, node: "A1")

    Fake.script_command(fake, "A1", {:ok, scripted_reply_body(0, 4, 60)})

    assert {:ok, %ExecuteTask{kind: :query_execute}} =
             Aerospike.query_execute(conn, query, [], node: "A1")

    Fake.script_command(fake, "A1", {:ok, scripted_reply_body(0, 4, 60)})

    assert {:ok, %ExecuteTask{kind: :query_udf}} =
             Aerospike.query_udf(conn, query, "pkg", "fun", [], node: "A1")
  end

  test "aggregate facade keeps partial streams distinct from finalized results", %{
    conn: conn,
    fake: fake
  } do
    query =
      Query.new(@namespace, "scan_ops")
      |> Query.where(Filter.range("payload", 0, 9))

    Fake.script_stream(fake, "A1", {:ok, [aggregate_frame(1), last_frame()]})
    Fake.script_stream(fake, "B1", {:ok, [aggregate_frame(2), last_frame()]})

    assert {:ok, partial_stream} = Aerospike.query_aggregate(conn, query, "pkg", "sum_values", [])
    assert Enum.sort(Enum.to_list(partial_stream)) == [1, 2]

    Fake.script_stream(fake, "A1", {:ok, [aggregate_frame(1), last_frame()]})
    Fake.script_stream(fake, "B1", {:ok, [aggregate_frame(2), last_frame()]})

    assert {:ok, 3} =
             Aerospike.query_aggregate_result(conn, query, "pkg", "sum_values", [],
               source: @aggregate_sum_source
             )
  end

  test "finalized aggregate facade validates local options before querying", %{conn: conn} do
    query =
      Query.new(@namespace, "scan_ops")
      |> Query.where(Filter.range("payload", 0, 9))

    assert {:error, %Aerospike.Error{code: :invalid_argument, message: missing_source}} =
             Aerospike.query_aggregate_result(conn, query, "pkg", "sum_values", [])

    assert missing_source =~ "missing aggregate Lua source"

    assert {:error, %Aerospike.Error{code: :invalid_argument, message: unsupported_node}} =
             Aerospike.query_aggregate_result(conn, query, "pkg", "sum_values", [],
               source: @aggregate_sum_source,
               node: "A1"
             )

    assert unsupported_node =~ ":node"

    assert_raise Aerospike.Error, ~r/missing aggregate Lua source/, fn ->
      Aerospike.query_aggregate_result!(conn, query, "pkg", "sum_values", [])
    end
  end

  test "bare scan aliases stay compatible with the explicit scan helpers", %{
    conn: conn,
    fake: fake
  } do
    scan = Scan.new(@namespace, "scan_ops")

    Fake.script_stream(fake, "A1", {:ok, [frame("alias-stream"), last_frame()]})

    assert {:ok, stream} = Aerospike.scan_stream(conn, scan, node: "A1")
    assert ["alias-stream"] = Enum.map(stream, & &1.bins["payload"])

    Fake.script_stream(fake, "A1", {:ok, [frame("alias-stream"), last_frame()]})

    alias_stream =
      :erlang.apply(Aerospike, :stream!, [conn, scan, [node: "A1"]])
      |> Enum.map(& &1.bins["payload"])

    assert ["alias-stream"] = alias_stream

    Fake.script_stream(fake, "A1", {:ok, [frame("alias-all"), last_frame()]})

    assert {:ok, [%{bins: %{"payload" => "alias-all"}}]} =
             Aerospike.scan_all(conn, scan, node: "A1")

    Fake.script_stream(fake, "A1", {:ok, [frame("alias-all"), last_frame()]})

    assert {:ok, [%{bins: %{"payload" => "alias-all"}}]} =
             :erlang.apply(Aerospike, :all, [conn, scan, [node: "A1"]])

    Fake.script_stream(fake, "A1", {:ok, [frame("alias-count"), last_frame()]})
    assert 1 = Aerospike.scan_count!(conn, scan, node: "A1")

    Fake.script_stream(fake, "A1", {:ok, [frame("alias-count"), last_frame()]})
    assert 1 = :erlang.apply(Aerospike, :count!, [conn, scan, [node: "A1"]])
  end

  test "bang wrappers raise the underlying public errors", %{conn: conn} do
    scan = Scan.new(@namespace, "scan_ops")
    query = Query.new(@namespace, "scan_ops") |> Query.where(Filter.range("payload", 0, 9))

    assert_raise Aerospike.Error, fn ->
      Aerospike.scan_stream!(conn, scan, node: "missing") |> Enum.to_list()
    end

    assert_raise Aerospike.Error, fn ->
      :erlang.apply(Aerospike, :stream!, [conn, scan, [node: "missing"]]) |> Enum.to_list()
    end

    assert_raise Aerospike.Error, ~r/max_records_required/i, fn ->
      Aerospike.query_all!(conn, query)
    end

    assert_raise Aerospike.Error, ~r/invalid cursor/i, fn ->
      Aerospike.query_page!(conn, Query.max_records(query, 1), cursor: 123)
    end
  end

  test "public query wrappers cover node streams, resumable pages, and background jobs", %{
    conn: conn,
    fake: fake
  } do
    query =
      Query.new(@namespace, "scan_ops")
      |> Query.where(Filter.range("payload", 0, 9))
      |> Query.max_records(1)

    Fake.script_stream(fake, "A1", {:ok, [frame("stream-node"), last_frame()]})

    assert [%{bins: %{"payload" => "stream-node"}}] =
             conn
             |> Aerospike.query_stream!(query, node: "A1")
             |> Enum.to_list()

    Fake.script_stream(
      fake,
      "A1",
      {:ok, [frame("page-1"), partition_done_frame("page-1"), last_frame()]}
    )

    Fake.script_stream(fake, "B1", {:ok, [last_frame()]})

    assert %{records: [%{bins: %{"payload" => "page-1"}}], cursor: %Cursor{} = cursor} =
             Aerospike.query_page!(conn, query)

    Fake.script_stream(
      fake,
      "A1",
      {:ok, [frame("page-2"), partition_done_frame("page-2"), last_frame()]}
    )

    Fake.script_stream(fake, "B1", {:ok, [last_frame()]})

    assert %{records: [%{bins: %{"payload" => "page-2"}}], cursor: %Cursor{}} =
             Aerospike.query_page!(conn, query, cursor: cursor)

    Fake.script_command(fake, "A1", {:ok, scripted_reply_body(0, 4, 60)})
    Fake.script_command(fake, "B1", {:ok, scripted_reply_body(0, 4, 60)})

    assert {:ok, %ExecuteTask{kind: :query_execute, node_name: nil}} =
             Aerospike.query_execute(conn, query, [])

    Fake.script_command(fake, "A1", {:ok, scripted_reply_body(0, 4, 60)})
    Fake.script_command(fake, "B1", {:ok, scripted_reply_body(0, 4, 60)})

    assert {:ok, %ExecuteTask{kind: :query_udf, node_name: nil}} =
             Aerospike.query_udf(conn, query, "pkg", "fun", [])
  end

  test "explicit node-targeted helpers cover scan and query bang paths", %{
    conn: conn,
    fake: fake
  } do
    scan = Scan.new(@namespace, "scan_ops")

    query =
      Query.new(@namespace, "scan_ops")
      |> Query.where(Filter.range("payload", 0, 9))
      |> Query.max_records(1)

    Fake.script_stream(fake, "A1", {:ok, [frame("node-scan"), last_frame()]})

    assert [%{bins: %{"payload" => "node-scan"}}] =
             Aerospike.scan_all!(conn, scan, node: "A1")

    Fake.script_stream(fake, "A1", {:ok, [frame("node-query"), last_frame()]})

    assert [%{bins: %{"payload" => "node-query"}}] =
             conn
             |> Aerospike.query_stream!(query, node: "A1")
             |> Enum.to_list()

    Fake.script_command(fake, "A1", {:ok, scripted_reply_body(0, 4, 60)})

    assert {:ok, %ExecuteTask{kind: :query_execute, node_name: "A1"}} =
             Aerospike.query_execute(conn, query, [], node: "A1")
  end

  test "transaction wrappers initialize tracking, abort on explicit errors, and reject reused handles",
       %{
         conn_name: conn_name
       } do
    assert {:ok, :done} =
             Aerospike.transaction(conn_name, fn txn ->
               assert {:ok, :open} = Aerospike.txn_status(conn_name, txn)
               :done
             end)

    txn = Txn.new()

    assert {:error, %Aerospike.Error{code: :timeout}} =
             Aerospike.transaction(conn_name, txn, fn tx ->
               assert {:ok, :open} = Aerospike.txn_status(conn_name, tx)
               raise Aerospike.Error.from_result_code(:timeout)
             end)

    assert {:error, %Aerospike.Error{code: :parameter_error}} = Aerospike.commit(conn_name, txn)
    assert {:error, %Aerospike.Error{code: :parameter_error}} = Aerospike.abort(conn_name, txn)
  end

  defp script_two_node_cluster(fake) do
    Fake.script_info(fake, "A1", ["node", "features"], %{"node" => "A1", "features" => ""})
    Fake.script_info(fake, "B1", ["node", "features"], %{"node" => "B1", "features" => ""})

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

    Fake.script_info(
      fake,
      "B1",
      ["partition-generation", "cluster-stable", "peers-generation"],
      %{
        "partition-generation" => "1",
        "cluster-stable" => "deadbeef",
        "peers-generation" => "1"
      }
    )

    Fake.script_info(fake, "A1", ["peers-clear-std"], %{"peers-clear-std" => "0,3000,[]"})
    Fake.script_info(fake, "B1", ["peers-clear-std"], %{"peers-clear-std" => "0,3000,[]"})

    Fake.script_info(fake, "A1", ["replicas"], %{
      "replicas" => ReplicasFixture.build(@namespace, 1, [Enum.to_list(0..100), []])
    })

    Fake.script_info(fake, "B1", ["replicas"], %{
      "replicas" => ReplicasFixture.build(@namespace, 1, [Enum.to_list(101..4095), []])
    })
  end

  defp frame(payload) do
    {:frame, encode_bin(record_msg(payload))}
  end

  defp aggregate_frame(value) do
    {:ok, {particle_type, data}} = Value.encode_value(value)
    {:frame, encode_bin(aggregate_msg(particle_type, data))}
  end

  defp partition_done_frame(payload) do
    {:frame, encode_bin(partition_done_msg(payload))}
  end

  defp last_frame do
    {:frame, encode_bin(%AsmMsg{info3: AsmMsg.info3_last()})}
  end

  defp record_msg(payload) do
    %AsmMsg{
      info1: AsmMsg.info1_read(),
      result_code: 0,
      generation: 7,
      expiration: 120,
      fields: [
        Field.namespace(@namespace),
        Field.set("scan_ops"),
        Field.digest(digest_fixture(payload))
      ],
      operations: [
        %Operation{
          op_type: Operation.op_read(),
          particle_type: 3,
          bin_name: "payload",
          data: payload
        }
      ]
    }
  end

  defp aggregate_msg(particle_type, data) do
    %AsmMsg{
      info1: AsmMsg.info1_read(),
      result_code: 0,
      fields: [],
      operations: [
        %Operation{
          op_type: Operation.op_read(),
          particle_type: particle_type,
          bin_name: "SUCCESS",
          data: data
        }
      ]
    }
  end

  defp partition_done_msg(payload) do
    %AsmMsg{
      info3: 0x04,
      result_code: 0,
      generation: 0,
      expiration: 0,
      fields: [Field.digest(digest_fixture(payload))],
      operations: []
    }
  end

  defp encode_bin(msg), do: IO.iodata_to_binary(Message.encode_as_msg_iodata(AsmMsg.encode(msg)))
  defp digest_fixture(seed), do: :crypto.hash(:ripemd160, seed)

  defp scripted_reply_body(result_code, generation, ttl) do
    <<22, 0, 0, 0, 0, result_code::8, generation::32-big, ttl::32-big, 0::32, 0::16, 0::16>>
  end

  defp udf_reply_body(bin_name, value, result_code \\ 0) do
    {:ok, operation} = AsmMsg.Operation.write(bin_name, value)

    %AsmMsg{result_code: result_code, operations: [operation]}
    |> AsmMsg.encode()
    |> IO.iodata_to_binary()
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

  defp uint32_field(id, value) when is_integer(id) and is_integer(value) do
    admin_field(id, <<value::32-big>>)
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
end
