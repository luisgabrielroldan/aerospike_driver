defmodule Aerospike.BatchTest do
  use ExUnit.Case, async: true

  alias Aerospike.Batch
  alias Aerospike.BatchCommand.Entry
  alias Aerospike.Error
  alias Aerospike.Key
  alias Aerospike.NodeSupervisor
  alias Aerospike.PartitionMapWriter
  alias Aerospike.Protocol.AsmMsg
  alias Aerospike.Protocol.AsmMsg.Operation
  alias Aerospike.TableOwner
  alias Aerospike.Tender
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
