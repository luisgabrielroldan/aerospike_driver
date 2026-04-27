defmodule Aerospike.Command.BatchGetTest do
  use ExUnit.Case, async: true

  alias Aerospike
  alias Aerospike.Cluster.NodeSupervisor
  alias Aerospike.Cluster.PartitionMapWriter
  alias Aerospike.Cluster.TableOwner
  alias Aerospike.Cluster.Tender
  alias Aerospike.Command.BatchGet
  alias Aerospike.Error
  alias Aerospike.Key
  alias Aerospike.Op
  alias Aerospike.Protocol.AsmMsg
  alias Aerospike.Protocol.AsmMsg.Operation
  alias Aerospike.Test.ReplicasFixture
  alias Aerospike.Transport.Fake

  setup context do
    name = :"batch_get_test_#{:erlang.phash2(context.test)}"

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

  describe "batch_get/4" do
    test "returns an empty result for an empty key list when opts are valid" do
      assert {:ok, []} = Aerospike.batch_get(:unused_tender, [])
    end

    test "returns per-key results in caller order across multiple nodes", ctx do
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
             %Operation{op_type: 1, particle_type: 3, bin_name: "name", data: "Ada"}
           ]),
           batch_row(2, 2, 0, 0, []),
           last_row()
         ])}
      )

      Fake.script_command_stream(
        ctx.fake,
        "B1",
        {:ok,
         IO.iodata_to_binary([
           batch_row(1, 0, 5, 240, [
             %Operation{
               op_type: 1,
               particle_type: 1,
               bin_name: "count",
               data: <<7::64-signed-big>>
             }
           ]),
           last_row()
         ])}
      )

      assert {:ok, [first, second, third]} =
               Aerospike.batch_get(tender, [key_a, key_b_tuple, key_c])

      assert {:ok, %{key: ^key_a, bins: %{"name" => "Ada"}, generation: 3, ttl: 120}} = first
      assert {:ok, %{key: ^key_b, bins: %{"count" => 7}, generation: 5, ttl: 240}} = second
      assert {:error, %Error{code: :key_not_found}} = third
    end

    test "keeps a node transport failure scoped to the affected key indices", ctx do
      script_two_node_cluster(ctx.fake)
      {:ok, tender} = start_tender(ctx)
      :ok = Tender.tend_now(tender)

      key_a = key_for_partition("test", "batch", 100, "a")
      key_b = key_for_partition("test", "batch", 101, "b")
      key_c = key_for_partition("test", "batch", 100, "c")

      Fake.script_command_stream(
        ctx.fake,
        "A1",
        {:error, Error.from_result_code(:network_error)}
      )

      Fake.script_command_stream(
        ctx.fake,
        "B1",
        {:ok,
         IO.iodata_to_binary([
           batch_row(1, 0, 5, 240, [
             %Operation{
               op_type: 1,
               particle_type: 1,
               bin_name: "count",
               data: <<7::64-signed-big>>
             }
           ]),
           last_row()
         ])}
      )

      assert {:ok, [first, second, third]} = Aerospike.batch_get(tender, [key_a, key_b, key_c])

      assert {:error, %Error{code: :network_error}} = first
      assert {:ok, %{key: ^key_b, bins: %{"count" => 7}}} = second
      assert {:error, %Error{code: :network_error}} = third
    end

    test "returns projected bin results in caller order", ctx do
      script_two_node_cluster(ctx.fake)
      {:ok, tender} = start_tender(ctx)
      :ok = Tender.tend_now(tender)

      key_a = key_for_partition("test", "batch", 100, "a")
      key_b = key_for_partition("test", "batch", 101, "b")
      key_c = key_for_partition("test", "batch", 100, "c")

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
        {:ok,
         IO.iodata_to_binary([
           batch_row(1, 0, 5, 240, [
             %Operation{
               op_type: 1,
               particle_type: 1,
               bin_name: "count",
               data: <<7::64-signed-big>>
             }
           ]),
           last_row()
         ])}
      )

      assert {:ok, [first, second, third]} =
               Aerospike.batch_get(tender, [key_a, key_b, key_c], ["name", :count])

      assert {:ok, %{key: ^key_a, bins: %{"name" => "Ada"}, generation: 3, ttl: 120}} = first
      assert {:ok, %{key: ^key_b, bins: %{"count" => 7}, generation: 5, ttl: 240}} = second
      assert {:error, %Error{code: :key_not_found}} = third
    end

    test "rejects unsupported batch opts instead of silently dropping them" do
      key = Key.new("test", "batch", "k1")

      assert {:error, %Error{code: :invalid_argument, message: message}} =
               Aerospike.batch_get(:unused_tender, [key], :all, max_retries: 1)

      assert message =~ "currently support only the :timeout option"
    end

    test "rejects invalid tuple keys at the public boundary" do
      assert {:error, %Error{code: :invalid_argument, message: message}} =
               Aerospike.batch_get(:unused_tender, [{"test", :batch, "k1"}])

      assert message =~ "set must be a string"
    end

    test "rejects direct non-key inputs after tuple coercion" do
      assert {:error, %Error{code: :invalid_argument, message: message}} =
               Aerospike.batch_get(:unused_tender, [:bad])

      assert message =~ "expected %Aerospike.Key{} or {namespace, set, user_key} tuple"
    end

    test "rejects unsupported batch modes on the internal execute boundary" do
      key = Key.new("test", "batch", "k1")

      assert {:error, %Error{code: :invalid_argument, message: message}} =
               BatchGet.execute(:unused_tender, [key], :bins, [])

      assert message =~ "expects :all, :header, :exists"
    end

    test "rejects invalid named-bin lists before dispatch" do
      key = Key.new("test", "batch", "k1")

      assert {:error, %Error{code: :invalid_argument, message: empty_message}} =
               BatchGet.execute(:unused_tender, [key], [], [])

      assert empty_message =~ "at least one named bin"

      assert {:error, %Error{code: :invalid_argument, message: blank_message}} =
               BatchGet.execute(:unused_tender, [key], [""], [])

      assert blank_message =~ "non-empty strings or atoms"

      assert {:error, %Error{code: :invalid_argument, message: type_message}} =
               BatchGet.execute(:unused_tender, [key], ["name", 1], [])

      assert type_message =~ "non-empty strings or atoms"
    end
  end

  describe "batch_get_operate/4" do
    test "returns an empty result for an empty key list when ops and opts are valid" do
      assert {:ok, []} = Aerospike.batch_get_operate(:unused_tender, [], [Op.get("name")])
    end

    test "rejects empty operation lists" do
      key = Key.new("test", "batch", "k1")

      assert {:error, %Error{code: :invalid_argument, message: message}} =
               Aerospike.batch_get_operate(:unused_tender, [key], [])

      assert message =~ "expects a non-empty operation list"
    end

    test "rejects write operation lists" do
      key = Key.new("test", "batch", "k1")

      assert {:error, %Error{code: :invalid_argument, message: message}} =
               Aerospike.batch_get_operate(:unused_tender, [key], [Op.put("name", "Ada")])

      assert message =~ "accepts only read-only operations"
    end

    test "rejects unsupported batch opts instead of silently dropping them" do
      key = Key.new("test", "batch", "k1")

      assert {:error, %Error{code: :invalid_argument, message: message}} =
               Aerospike.batch_get_operate(:unused_tender, [key], [Op.get("name")],
                 max_retries: 1
               )

      assert message =~ "currently support only the :timeout option"
    end

    test "rejects invalid tuple keys at the public boundary" do
      assert {:error, %Error{code: :invalid_argument, message: message}} =
               Aerospike.batch_get_operate(:unused_tender, [{"test", :batch, "k1"}], [
                 Op.get("name")
               ])

      assert message =~ "set must be a string"
    end

    test "returns per-key records and errors in caller order across multiple nodes", ctx do
      script_two_node_cluster(ctx.fake)
      {:ok, tender} = start_tender(ctx)
      :ok = Tender.tend_now(tender)

      key_a = key_for_partition("test", "batch", 100, "a")
      key_b = key_for_partition("test", "batch", 101, "b")
      key_c = key_for_partition("test", "batch", 100, "c")
      key_d = key_for_partition("test", "batch", 101, "d")
      key_b_tuple = {key_b.namespace, key_b.set, key_b.user_key}

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
        {:ok,
         IO.iodata_to_binary([
           batch_row(1, 0, 5, 240, [
             %Operation{
               op_type: 1,
               particle_type: 1,
               bin_name: "count",
               data: <<7::64-signed-big>>
             }
           ]),
           batch_row(3, 4, 0, 0, []),
           last_row()
         ])}
      )

      assert {:ok, [first, second, third, fourth]} =
               Aerospike.batch_get_operate(
                 tender,
                 [key_a, key_b_tuple, key_c, key_d],
                 [Op.get("name")]
               )

      assert {:ok, %{key: ^key_a, bins: %{"name" => "Ada"}, generation: 3, ttl: 120}} = first
      assert {:ok, %{key: ^key_b, bins: %{"count" => 7}, generation: 5, ttl: 240}} = second
      assert {:error, %Error{code: :key_not_found}} = third
      assert {:error, %Error{code: :parameter_error}} = fourth
    end
  end

  describe "batch_get_header/3" do
    test "returns header-only records with empty bins in caller order", ctx do
      script_two_node_cluster(ctx.fake)
      {:ok, tender} = start_tender(ctx)
      :ok = Tender.tend_now(tender)

      key_a = key_for_partition("test", "batch", 100, "a")
      key_b = key_for_partition("test", "batch", 101, "b")
      key_c = key_for_partition("test", "batch", 100, "c")

      Fake.script_command_stream(
        ctx.fake,
        "A1",
        {:ok,
         IO.iodata_to_binary([batch_row(0, 0, 3, 120, []), batch_row(2, 2, 0, 0, []), last_row()])}
      )

      Fake.script_command_stream(
        ctx.fake,
        "B1",
        {:ok, IO.iodata_to_binary([batch_row(1, 0, 5, 240, []), last_row()])}
      )

      assert {:ok, [first, second, third]} =
               Aerospike.batch_get_header(tender, [key_a, key_b, key_c])

      assert {:ok, %{key: ^key_a, bins: %{}, generation: 3, ttl: 120}} = first
      assert {:ok, %{key: ^key_b, bins: %{}, generation: 5, ttl: 240}} = second
      assert {:error, %Error{code: :key_not_found}} = third
    end
  end

  describe "batch_exists/3" do
    test "returns booleans for hits and misses while preserving caller order", ctx do
      script_two_node_cluster(ctx.fake)
      {:ok, tender} = start_tender(ctx)
      :ok = Tender.tend_now(tender)

      key_a = key_for_partition("test", "batch", 100, "a")
      key_b = key_for_partition("test", "batch", 101, "b")
      key_c = key_for_partition("test", "batch", 100, "c")

      Fake.script_command_stream(
        ctx.fake,
        "A1",
        {:ok,
         IO.iodata_to_binary([batch_row(0, 0, 3, 120, []), batch_row(2, 2, 0, 0, []), last_row()])}
      )

      Fake.script_command_stream(
        ctx.fake,
        "B1",
        {:ok, IO.iodata_to_binary([batch_row(1, 0, 5, 240, []), last_row()])}
      )

      assert {:ok, [first, second, third]} = Aerospike.batch_exists(tender, [key_a, key_b, key_c])

      assert {:ok, true} = first
      assert {:ok, true} = second
      assert {:ok, false} = third
    end

    test "keeps a node transport failure scoped to the affected key indices", ctx do
      script_two_node_cluster(ctx.fake)
      {:ok, tender} = start_tender(ctx)
      :ok = Tender.tend_now(tender)

      key_a = key_for_partition("test", "batch", 100, "a")
      key_b = key_for_partition("test", "batch", 101, "b")
      key_c = key_for_partition("test", "batch", 100, "c")

      Fake.script_command_stream(
        ctx.fake,
        "A1",
        {:error, Error.from_result_code(:network_error)}
      )

      Fake.script_command_stream(
        ctx.fake,
        "B1",
        {:ok, IO.iodata_to_binary([batch_row(1, 0, 5, 240, []), last_row()])}
      )

      assert {:ok, [first, second, third]} = Aerospike.batch_exists(tender, [key_a, key_b, key_c])

      assert {:error, %Error{code: :network_error}} = first
      assert {:ok, true} = second
      assert {:error, %Error{code: :network_error}} = third
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
