defmodule Aerospike.PutPayloadTest do
  use ExUnit.Case, async: false

  alias Aerospike.Cluster.NodeSupervisor
  alias Aerospike.Cluster.PartitionMapWriter
  alias Aerospike.Cluster.TableOwner
  alias Aerospike.Cluster.Tender
  alias Aerospike.Command.PutPayload
  alias Aerospike.Error
  alias Aerospike.Key
  alias Aerospike.Runtime.TxnOps
  alias Aerospike.Test.ReplicasFixture
  alias Aerospike.Transport.Fake
  alias Aerospike.Txn

  @namespace "test"
  @set "spike"

  setup context do
    name = :"put_payload_#{:erlang.phash2(context.test)}"

    {:ok, fake} = Fake.start_link(nodes: [{"A1", "10.0.0.1", 3000}])
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

    %{fake: fake, name: name, node_sup_name: NodeSupervisor.sup_name(name), tables: tables}
  end

  test "forwards caller payload bytes unchanged and disables compression", ctx do
    script_bootstrap_node(ctx.fake, "A1", ReplicasFixture.all_master(@namespace, 1),
      features: "compression"
    )

    {:ok, tender} = start_tender(ctx, use_compression: true)
    :ok = Tender.tend_now(tender)

    payload = :crypto.strong_rand_bytes(256)
    key = Key.new(@namespace, @set, "raw-payload")

    Fake.script_command(ctx.fake, "A1", {:ok, scripted_reply_body(0)})

    assert :ok = Aerospike.put_payload(tender, key, payload)
    assert Fake.last_command_request(ctx.fake, "A1") == payload
    assert Keyword.fetch!(Fake.last_command_opts(ctx.fake, "A1"), :use_compression) == false
  end

  test "empty binary reaches the routed transport hook", ctx do
    script_bootstrap_node(ctx.fake, "A1", ReplicasFixture.all_master(@namespace, 1))
    {:ok, tender} = start_tender(ctx)
    :ok = Tender.tend_now(tender)

    key = Key.new(@namespace, @set, "empty-payload")
    Fake.script_command(ctx.fake, "A1", {:ok, scripted_reply_body(0)})

    assert :ok = PutPayload.execute(tender, key, <<>>)
    assert Fake.last_command_request(ctx.fake, "A1") == <<>>
  end

  test "returns key and option validation errors" do
    assert {:error, %Error{code: :invalid_argument, message: key_message}} =
             Aerospike.put_payload(:cluster, {:bad}, <<>>)

    assert key_message =~ "%Aerospike.Key{}"
  end

  test "returns option validation errors before dispatch", ctx do
    {:ok, tender} = start_tender(ctx)
    key = Key.new(@namespace, @set, "bad-options")

    assert {:error, %Error{code: :invalid_argument, message: opt_message}} =
             Aerospike.put_payload(tender, key, <<>>, timeout: -1)

    assert opt_message =~ "timeout must be a non-negative integer"
  end

  test "non-binary payloads do not match the public facade" do
    key = Key.new(@namespace, @set, "non-binary")

    assert_raise FunctionClauseError, fn ->
      Aerospike.put_payload(:cluster, key, :not_binary)
    end
  end

  test "does not register transaction writes or add monitor fields", ctx do
    script_bootstrap_node(ctx.fake, "A1", ReplicasFixture.all_master(@namespace, 1))
    {:ok, tender} = start_tender(ctx)
    :ok = Tender.tend_now(tender)

    txn = Txn.new(timeout: 5_000)
    key = Key.new(@namespace, @set, "txn-bypass")
    payload = <<"caller-owned-frame">>

    TxnOps.init_tracking(ctx.name, txn)
    Fake.script_command(ctx.fake, "A1", {:ok, scripted_reply_body(0)})

    assert TxnOps.get_writes(ctx.name, txn) == []
    assert :ok = Aerospike.put_payload(tender, key, payload, txn: txn)
    assert TxnOps.get_writes(ctx.name, txn) == []
    assert Fake.last_command_request(ctx.fake, "A1") == payload
  end

  defp script_bootstrap_node(fake, node_name, replicas_value, opts \\ []) do
    Fake.script_info(fake, node_name, ["node", "features"], %{
      "node" => node_name,
      "features" => Keyword.get(opts, :features, "")
    })

    partitions =
      ["partition-generation", "cluster-stable", "peers-generation"]

    Fake.script_info(fake, node_name, partitions, %{
      "partition-generation" => "1",
      "cluster-stable" => "deadbeef",
      "peers-generation" => "1"
    })

    Fake.script_info(fake, node_name, ["peers-clear-std"], %{
      "peers-clear-std" => "0,3000,[]"
    })

    Fake.script_info(fake, node_name, ["replicas"], %{"replicas" => replicas_value})
  end

  defp start_tender(ctx, overrides \\ []) do
    base_opts = [
      name: ctx.name,
      transport: Fake,
      connect_opts: [fake: ctx.fake],
      seeds: [{"10.0.0.1", 3000}],
      namespaces: [@namespace],
      tables: ctx.tables,
      tend_trigger: :manual,
      node_supervisor: ctx.node_sup_name,
      pool_size: 1
    ]

    {:ok, pid} = Tender.start_link(Keyword.merge(base_opts, overrides))
    on_exit(fn -> stop_quietly(pid) end)
    {:ok, pid}
  end

  defp scripted_reply_body(result_code, generation \\ 0, ttl \\ 0) do
    <<22, 0, 0, 0, 0, result_code::8, generation::32-big, ttl::32-big, 0::32, 0::16, 0::16>>
  end

  defp stop_quietly(pid) do
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
