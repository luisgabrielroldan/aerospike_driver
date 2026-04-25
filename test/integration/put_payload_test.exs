defmodule Aerospike.Integration.PutPayloadTest do
  use ExUnit.Case, async: false

  @moduletag :integration

  alias Aerospike.Error
  alias Aerospike.Key
  alias Aerospike.Protocol.AsmMsg
  alias Aerospike.Protocol.AsmMsg.Operation
  alias Aerospike.Protocol.Message
  alias Aerospike.Record
  alias Aerospike.Test.IntegrationSupport

  @host "localhost"
  @port 3000
  @namespace "test"
  @set "spike"

  setup do
    IntegrationSupport.probe_aerospike!(@host, @port)
    IntegrationSupport.wait_for_seed_ready!(@host, @port, @namespace, 5_000)
    name = IntegrationSupport.unique_atom("spike_put_payload_cluster")

    {:ok, sup} =
      Aerospike.start_link(
        name: name,
        transport: Aerospike.Transport.Tcp,
        hosts: ["#{@host}:#{@port}"],
        namespaces: [@namespace],
        tend_trigger: :manual,
        pool_size: 2
      )

    IntegrationSupport.wait_for_tender_ready!(name, 5_000)

    on_exit(fn -> IntegrationSupport.stop_supervisor_quietly(sup) end)

    %{cluster: name}
  end

  test "replays caller-built write and delete frames", %{cluster: cluster} do
    write_key = IntegrationSupport.unique_key(@namespace, @set, "put_payload_write")
    delete_key = IntegrationSupport.unique_key(@namespace, @set, "put_payload_delete")

    on_exit(fn ->
      cleanup_key(cluster, write_key)
      cleanup_key(cluster, delete_key)
    end)

    assert :ok = Aerospike.put_payload(cluster, write_key, write_payload(write_key, "value"))

    assert {:ok, %Record{bins: %{"payload" => "value"}}} =
             Aerospike.get(cluster, write_key)

    assert {:ok, %{generation: generation}} =
             Aerospike.put(cluster, delete_key, %{"payload" => "delete"})

    assert generation >= 1
    assert :ok = Aerospike.put_payload(cluster, delete_key, delete_payload(delete_key))
    assert {:ok, false} = Aerospike.exists(cluster, delete_key)
  end

  test "maps generation mismatch errors from the standard write response", %{cluster: cluster} do
    key = IntegrationSupport.unique_key(@namespace, @set, "put_payload_generation")

    on_exit(fn -> cleanup_key(cluster, key) end)

    assert {:ok, %{generation: generation}} = Aerospike.put(cluster, key, %{"payload" => "seed"})

    payload = write_payload(key, "stale", generation: generation + 100)

    assert {:error, %Error{code: :generation_error}} =
             Aerospike.put_payload(cluster, key, payload)

    assert {:ok, %Record{bins: %{"payload" => "seed"}}} = Aerospike.get(cluster, key)
  end

  test "bang variant returns :ok on success", %{cluster: cluster} do
    key = IntegrationSupport.unique_key(@namespace, @set, "put_payload_bang")

    on_exit(fn -> cleanup_key(cluster, key) end)

    assert :ok = Aerospike.put_payload!(cluster, key, write_payload(key, "bang"))
    assert {:ok, %Record{bins: %{"payload" => "bang"}}} = Aerospike.get(cluster, key)
  end

  defp write_payload(%Key{} = key, value, opts \\ []) do
    {:ok, operation} = Operation.write("payload", value)

    key
    |> AsmMsg.key_command([operation],
      write: true,
      send_key: true,
      generation: Keyword.get(opts, :generation, 0)
    )
    |> AsmMsg.encode()
    |> Message.encode_as_msg_iodata()
    |> IO.iodata_to_binary()
  end

  defp delete_payload(%Key{} = key) do
    key
    |> AsmMsg.key_command([], write: true, delete: true, send_key: true)
    |> AsmMsg.encode()
    |> Message.encode_as_msg_iodata()
    |> IO.iodata_to_binary()
  end

  defp cleanup_key(cluster, %Key{} = key) do
    Aerospike.delete(cluster, key)
  catch
    :exit, _ -> :ok
  end
end
