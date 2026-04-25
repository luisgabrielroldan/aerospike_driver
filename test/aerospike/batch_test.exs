defmodule Aerospike.Command.BatchTest do
  use ExUnit.Case, async: true

  alias Aerospike.Batch, as: BatchEntry
  alias Aerospike.BatchResult
  alias Aerospike.Cluster.NodeSupervisor
  alias Aerospike.Cluster.PartitionMapWriter
  alias Aerospike.Cluster.TableOwner
  alias Aerospike.Cluster.Tender
  alias Aerospike.Command.Batch
  alias Aerospike.Command.BatchCommand.Entry
  alias Aerospike.Command.BatchDelete
  alias Aerospike.Command.BatchUdf
  alias Aerospike.Error
  alias Aerospike.Key
  alias Aerospike.Protocol.AsmMsg
  alias Aerospike.Protocol.AsmMsg.Operation
  alias Aerospike.Test.ReplicasFixture
  alias Aerospike.Transport.Fake

  setup context do
    name = :"batch_test_#{:erlang.phash2(context.test)}"

    {:ok, fake} = Fake.start_link(nodes: [{"A1", "10.0.0.1", 3000}, {"B1", "10.0.0.2", 3000}])
    {:ok, owner} = TableOwner.start_link(name: name)
    tables = TableOwner.tables(owner)
    {:ok, writer} = PartitionMapWriter.start_link(name: name, tables: tables)
    {:ok, node_sup} = NodeSupervisor.start_link(name: name)

    on_exit(fn ->
      stop_quietly(node_sup)
      stop_quietly(writer)
      stop_quietly(owner)
      stop_quietly(fake)
    end)

    %{name: name, fake: fake, tables: tables, node_sup_name: NodeSupervisor.sup_name(name)}
  end

  describe "Aerospike.Batch constructors" do
    test "coerce tuple keys and expose the targeted key" do
      key = Key.new("test", "batch", "k1")
      tuple = {key.namespace, key.set, key.user_key}

      assert %BatchEntry.Read{key: ^key} = BatchEntry.read(tuple)
      assert %BatchEntry.Put{key: ^key, bins: %{n: 1}} = BatchEntry.put(tuple, %{n: 1})
      assert %BatchEntry.Delete{key: ^key} = BatchEntry.delete(tuple)
      assert %BatchEntry.Operate{key: ^key, operations: []} = BatchEntry.operate(tuple, [])

      assert %BatchEntry.UDF{key: ^key, package: "pkg", function: "fun", args: []} =
               BatchEntry.udf(tuple, "pkg", "fun")

      assert BatchEntry.key(BatchEntry.read(key)) == key
      assert BatchEntry.key(BatchEntry.put(key, %{n: 1})) == key
      assert BatchEntry.key(BatchEntry.delete(key)) == key
      assert BatchEntry.key(BatchEntry.operate(key, [])) == key
      assert BatchEntry.key(BatchEntry.udf(key, "pkg", "fun", ["arg"])) == key
    end
  end

  test "executes mixed grouped entries through one substrate and preserves caller order", ctx do
    script_two_node_cluster(ctx.fake)
    {:ok, tender} = start_tender(ctx)
    :ok = Tender.tend_now(tender)

    key_read = key_for_partition("test", "batch", 100, "read")
    key_delete = key_for_partition("test", "batch", 101, "delete")
    key_exists = key_for_partition("test", "batch", 100, "exists")

    Fake.script_command_stream(
      ctx.fake,
      "A1",
      {:ok,
       IO.iodata_to_binary([
         batch_row(0, 0, 3, 120, [
           %Operation{op_type: 1, particle_type: 3, bin_name: "name", data: "Ada"}
         ]),
         batch_row(2, 2, 0, 0, []),
         last_row()
       ])}
    )

    Fake.script_command_stream(
      ctx.fake,
      "B1",
      {:ok, IO.iodata_to_binary([batch_row(1, 0, 0, 0, []), last_row()])}
    )

    entries = [
      %Entry{index: 0, key: key_read, kind: :read, dispatch: {:read, :master, 0}, payload: nil},
      %Entry{index: 1, key: key_delete, kind: :delete, dispatch: :write, payload: nil},
      %Entry{
        index: 2,
        key: key_exists,
        kind: :exists,
        dispatch: {:read, :master, 0},
        payload: nil
      }
    ]

    assert {:ok, [first, second, third]} = Batch.execute(tender, entries)

    assert %{
             index: 0,
             key: ^key_read,
             kind: :read,
             status: :ok,
             record: %{bins: %{"name" => "Ada"}}
           } =
             first

    assert %{index: 1, key: ^key_delete, kind: :delete, status: :ok, record: nil, error: nil} =
             second

    assert %{index: 2, key: ^key_exists, kind: :exists, status: :error, record: nil} = third
    assert %Error{code: :key_not_found} = third.error

    assert [
             %BatchResult{
               key: ^key_read,
               status: :ok,
               record: %{bins: %{"name" => "Ada"}},
               error: nil,
               in_doubt: false
             },
             %BatchResult{
               key: ^key_delete,
               status: :ok,
               record: nil,
               error: nil,
               in_doubt: false
             },
             %BatchResult{
               key: ^key_exists,
               status: :error,
               record: nil,
               error: %Error{code: :key_not_found},
               in_doubt: false
             }
           ] = Batch.to_public_results([first, second, third])
  end

  describe "batch_operate/3" do
    test "returns an empty result for an empty entry list when opts are valid" do
      assert {:ok, []} = Aerospike.batch_operate(:unused_tender, [])
    end

    test "rejects unsupported batch opts before resolving the cluster" do
      key = Key.new("test", "batch", "k1")

      assert {:error, %Error{code: :invalid_argument, message: message}} =
               Aerospike.batch_operate(:unused_tender, [BatchEntry.read(key)], max_retries: 1)

      assert message =~ "currently support only the :timeout option"
    end

    test "returns caller-ordered public results for mixed entries", ctx do
      script_two_node_cluster(ctx.fake)
      {:ok, tender} = start_tender(ctx)
      :ok = Tender.tend_now(tender)

      key_read = key_for_partition("test", "batch", 100, "read")
      key_put = key_for_partition("test", "batch", 101, "put")
      key_delete = key_for_partition("test", "batch", 100, "delete")
      key_operate = key_for_partition("test", "batch", 101, "operate")
      key_udf = key_for_partition("test", "batch", 100, "udf")
      key_put_tuple = {key_put.namespace, key_put.set, key_put.user_key}

      {:ok, add_op} = Operation.add("count", 2)
      read_op = Operation.read("count")

      Fake.script_command_stream(
        ctx.fake,
        "A1",
        {:ok,
         IO.iodata_to_binary([
           batch_row(0, 0, 3, 120, [
             %Operation{op_type: 1, particle_type: 3, bin_name: "name", data: "Ada"}
           ]),
           batch_row(2, 0, 0, 0, []),
           batch_row(4, 100, 0, 0, []),
           last_row()
         ])}
      )

      Fake.script_command_stream(
        ctx.fake,
        "B1",
        {:ok,
         IO.iodata_to_binary([
           batch_row(1, 0, 0, 0, []),
           batch_row(3, 0, 7, 90, [
             %Operation{op_type: 1, particle_type: 1, bin_name: "count", data: <<3::64-big>>}
           ]),
           last_row()
         ])}
      )

      assert {:ok, [read, put, delete, operate, udf]} =
               Aerospike.batch_operate(tender, [
                 BatchEntry.read(key_read),
                 BatchEntry.put(key_put_tuple, %{name: "Grace"}),
                 BatchEntry.delete(key_delete),
                 BatchEntry.operate(key_operate, [add_op, read_op]),
                 BatchEntry.udf(key_udf, "missing_pkg", "missing_fn", [])
               ])

      assert %BatchResult{
               key: ^key_read,
               status: :ok,
               record: %{bins: %{"name" => "Ada"}},
               error: nil,
               in_doubt: false
             } = read

      assert %BatchResult{key: ^key_put, status: :ok, record: nil, error: nil} = put
      assert %BatchResult{key: ^key_delete, status: :ok, record: nil, error: nil} = delete

      assert %BatchResult{
               key: ^key_operate,
               status: :ok,
               record: %{bins: %{"count" => 3}},
               error: nil
             } = operate

      assert %BatchResult{key: ^key_udf, status: :error, record: nil, in_doubt: false} = udf
      assert %Error{code: :udf_bad_response} = udf.error
    end

    test "rejects invalid entries, empty puts, and empty operations", ctx do
      script_two_node_cluster(ctx.fake)
      {:ok, tender} = start_tender(ctx)
      :ok = Tender.tend_now(tender)

      key = key_for_partition("test", "batch", 100, "k")

      assert {:error, %Error{code: :invalid_argument, message: message}} =
               Aerospike.batch_operate(tender, [:bad_entry])

      assert message =~ "expects entries built by Aerospike.Batch"

      assert {:error, %Error{code: :invalid_argument, message: message}} =
               Aerospike.batch_operate(tender, [BatchEntry.put(key, %{})])

      assert message =~ "requires at least one bin write"

      assert {:error, %Error{code: :invalid_argument, message: message}} =
               Aerospike.batch_operate(tender, [BatchEntry.operate(key, [])])

      assert message =~ "requires at least one operation"
    end
  end

  describe "batch_delete/3" do
    test "returns an empty result for an empty key list when opts are valid" do
      assert {:ok, []} = Aerospike.batch_delete(:unused_tender, [])
    end

    test "rejects unsupported batch opts instead of silently dropping them" do
      key = Key.new("test", "batch", "k1")

      assert {:error, %Error{code: :invalid_argument, message: message}} =
               Aerospike.batch_delete(:unused_tender, [key], max_retries: 1)

      assert message =~ "currently support only the :timeout option"
    end

    test "rejects invalid tuple keys at the public boundary" do
      assert {:error, %Error{code: :invalid_argument, message: message}} =
               Aerospike.batch_delete(:unused_tender, [{"test", :batch, "k1"}])

      assert message =~ "set must be a string"
    end

    test "rejects non-key values at the command boundary" do
      key = Key.new("test", "batch", "k1")

      assert {:error, %Error{code: :invalid_argument, message: message}} =
               BatchDelete.execute(:unused_tender, [key, :not_a_key])

      assert message =~ "expects a list of %Aerospike.Key{} values"
    end

    test "returns caller-ordered public results across multiple nodes", ctx do
      script_two_node_cluster(ctx.fake)
      {:ok, tender} = start_tender(ctx)
      :ok = Tender.tend_now(tender)

      key_a = key_for_partition("test", "batch", 100, "a")
      key_b = key_for_partition("test", "batch", 101, "b")
      key_c = key_for_partition("test", "batch", 100, "c")
      key_b_tuple = {key_b.namespace, key_b.set, key_b.user_key}

      Fake.script_command_stream(
        ctx.fake,
        "A1",
        {:ok,
         IO.iodata_to_binary([batch_row(0, 0, 0, 0, []), batch_row(2, 2, 0, 0, []), last_row()])}
      )

      Fake.script_command_stream(
        ctx.fake,
        "B1",
        {:ok, IO.iodata_to_binary([batch_row(1, 0, 0, 0, []), last_row()])}
      )

      assert {:ok, [first, second, third]} =
               Aerospike.batch_delete(tender, [key_a, key_b_tuple, key_c])

      assert %BatchResult{key: ^key_a, status: :ok, record: nil, error: nil, in_doubt: false} =
               first

      assert %BatchResult{key: ^key_b, status: :ok, record: nil, error: nil, in_doubt: false} =
               second

      assert %BatchResult{key: ^key_c, status: :error, record: nil, in_doubt: false} = third
      assert %Error{code: :key_not_found} = third.error
    end

    test "keeps node transport failures scoped to affected key indices", ctx do
      script_two_node_cluster(ctx.fake)
      {:ok, tender} = start_tender(ctx)
      :ok = Tender.tend_now(tender)

      key_a = key_for_partition("test", "batch", 100, "a")
      key_b = key_for_partition("test", "batch", 101, "b")
      key_c = key_for_partition("test", "batch", 100, "c")

      Fake.script_command_stream(ctx.fake, "A1", {:error, Error.from_result_code(:network_error)})

      Fake.script_command_stream(
        ctx.fake,
        "B1",
        {:ok, IO.iodata_to_binary([batch_row(1, 0, 0, 0, []), last_row()])}
      )

      assert {:ok, [first, second, third]} = Aerospike.batch_delete(tender, [key_a, key_b, key_c])

      assert %BatchResult{key: ^key_a, status: :error, record: nil} = first
      assert %Error{code: :network_error} = first.error
      assert %BatchResult{key: ^key_b, status: :ok, record: nil, error: nil} = second
      assert %BatchResult{key: ^key_c, status: :error, record: nil} = third
      assert %Error{code: :network_error} = third.error
    end

    test "keeps routing failures scoped to affected key indices", ctx do
      script_two_node_cluster(ctx.fake)
      {:ok, tender} = start_tender(ctx)
      :ok = Tender.tend_now(tender)

      key_a = Key.new("missing", "batch", "a")
      key_b = key_for_partition("test", "batch", 101, "b")

      Fake.script_command_stream(
        ctx.fake,
        "B1",
        {:ok, IO.iodata_to_binary([batch_row(1, 0, 0, 0, []), last_row()])}
      )

      assert {:ok, [first, second]} = Aerospike.batch_delete(tender, [key_a, key_b])

      assert %BatchResult{key: ^key_a, status: :error, record: nil, error: :no_master} = first
      assert %BatchResult{key: ^key_b, status: :ok, record: nil, error: nil} = second
    end
  end

  describe "batch_udf/6" do
    test "returns an empty result for an empty key list when opts are valid" do
      assert {:ok, []} = Aerospike.batch_udf(:unused_tender, [], "pkg", "fun", [])
    end

    test "rejects unsupported batch opts instead of silently dropping them" do
      key = Key.new("test", "batch", "k1")

      assert {:error, %Error{code: :invalid_argument, message: message}} =
               Aerospike.batch_udf(:unused_tender, [key], "pkg", "fun", [], max_retries: 1)

      assert message =~ "currently support only the :timeout option"
    end

    test "rejects invalid tuple keys at the public boundary" do
      assert {:error, %Error{code: :invalid_argument, message: message}} =
               Aerospike.batch_udf(:unused_tender, [{"test", :batch, "k1"}], "pkg", "fun", [])

      assert message =~ "set must be a string"
    end

    test "rejects non-key values at the command boundary" do
      key = Key.new("test", "batch", "k1")

      assert {:error, %Error{code: :invalid_argument, message: message}} =
               BatchUdf.execute(:unused_tender, [key, :not_a_key], "pkg", "fun", [])

      assert message =~ "expects a list of %Aerospike.Key{} values"
    end

    test "returns caller-ordered public results across multiple nodes", ctx do
      script_two_node_cluster(ctx.fake)
      {:ok, tender} = start_tender(ctx)
      :ok = Tender.tend_now(tender)

      key_a = key_for_partition("test", "batch", 100, "a")
      key_b = key_for_partition("test", "batch", 101, "b")
      key_c = key_for_partition("test", "batch", 100, "c")
      key_b_tuple = {key_b.namespace, key_b.set, key_b.user_key}

      Fake.script_command_stream(
        ctx.fake,
        "A1",
        {:ok,
         IO.iodata_to_binary([
           batch_row(0, 0, 3, 120, [
             %Operation{op_type: 1, particle_type: 3, bin_name: "SUCCESS", data: "a"}
           ]),
           batch_row(2, 100, 0, 0, []),
           last_row()
         ])}
      )

      Fake.script_command_stream(
        ctx.fake,
        "B1",
        {:ok,
         IO.iodata_to_binary([
           batch_row(1, 0, 7, 90, [
             %Operation{op_type: 1, particle_type: 1, bin_name: "SUCCESS", data: <<2::64-big>>}
           ]),
           last_row()
         ])}
      )

      assert {:ok, [first, second, third]} =
               Aerospike.batch_udf(tender, [key_a, key_b_tuple, key_c], "pkg", "fun", ["arg"])

      assert %BatchResult{
               key: ^key_a,
               status: :ok,
               record: %{bins: %{"SUCCESS" => "a"}, generation: 3, ttl: 120},
               error: nil,
               in_doubt: false
             } = first

      assert %BatchResult{
               key: ^key_b,
               status: :ok,
               record: %{bins: %{"SUCCESS" => 2}, generation: 7, ttl: 90},
               error: nil,
               in_doubt: false
             } = second

      assert %BatchResult{key: ^key_c, status: :error, record: nil, in_doubt: false} = third
      assert %Error{code: :udf_bad_response} = third.error
    end

    test "keeps node transport failures scoped to affected key indices", ctx do
      script_two_node_cluster(ctx.fake)
      {:ok, tender} = start_tender(ctx)
      :ok = Tender.tend_now(tender)

      key_a = key_for_partition("test", "batch", 100, "a")
      key_b = key_for_partition("test", "batch", 101, "b")
      key_c = key_for_partition("test", "batch", 100, "c")

      Fake.script_command_stream(ctx.fake, "A1", {:error, Error.from_result_code(:network_error)})

      Fake.script_command_stream(
        ctx.fake,
        "B1",
        {:ok, IO.iodata_to_binary([batch_row(1, 100, 0, 0, []), last_row()])}
      )

      assert {:ok, [first, second, third]} =
               Aerospike.batch_udf(tender, [key_a, key_b, key_c], "pkg", "fun", [])

      assert %BatchResult{key: ^key_a, status: :error, record: nil} = first
      assert %Error{code: :network_error} = first.error
      assert %BatchResult{key: ^key_b, status: :error, record: nil} = second
      assert %Error{code: :udf_bad_response} = second.error
      assert %BatchResult{key: ^key_c, status: :error, record: nil} = third
      assert %Error{code: :network_error} = third.error
    end

    test "keeps routing failures scoped to affected key indices", ctx do
      script_two_node_cluster(ctx.fake)
      {:ok, tender} = start_tender(ctx)
      :ok = Tender.tend_now(tender)

      key_a = Key.new("missing", "batch", "a")
      key_b = key_for_partition("test", "batch", 101, "b")

      Fake.script_command_stream(
        ctx.fake,
        "B1",
        {:ok, IO.iodata_to_binary([batch_row(1, 0, 0, 0, []), last_row()])}
      )

      assert {:ok, [first, second]} =
               Aerospike.batch_udf(tender, [key_a, key_b], "pkg", "fun", [])

      assert %BatchResult{key: ^key_a, status: :error, record: nil, error: :no_master} = first
      assert %BatchResult{key: ^key_b, status: :ok, record: nil, error: nil} = second
    end
  end

  defp start_tender(ctx) do
    {:ok, pid} =
      Tender.start_link(
        name: ctx.name,
        transport: Fake,
        connect_opts: [fake: ctx.fake],
        seeds: [{"10.0.0.1", 3000}, {"10.0.0.2", 3000}],
        namespaces: ["test"],
        tables: ctx.tables,
        tend_trigger: :manual,
        node_supervisor: ctx.node_sup_name,
        pool_size: 1
      )

    on_exit(fn -> stop_quietly(pid) end)
    {:ok, pid}
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
      "replicas" => ReplicasFixture.build("test", 1, [Enum.to_list(0..100), []])
    })

    Fake.script_info(fake, "B1", ["replicas"], %{
      "replicas" => ReplicasFixture.build("test", 1, [Enum.to_list(101..4095), []])
    })
  end

  defp key_for_partition(namespace, set, partition_id, seed) do
    Stream.iterate(0, &(&1 + 1))
    |> Stream.map(&Key.new(namespace, set, "#{seed}-#{&1}"))
    |> Enum.find(&(Key.partition_id(&1) == partition_id))
  end

  defp batch_row(index, result_code, generation, expiration, operations) do
    [
      <<22::8, 0::8, 0::8, 0::8, 0::8, result_code::8, generation::32-big, expiration::32-big,
        index::32-big, 0::16-big, length(operations)::16-big>>,
      Enum.map(operations, &Operation.encode/1)
    ]
  end

  defp last_row do
    <<22::8, 0::8, 0::8, AsmMsg.info3_last()::8, 0::8, 0::8, 0::32-big, 0::32-big, 0::32-big,
      0::16-big, 0::16-big>>
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
