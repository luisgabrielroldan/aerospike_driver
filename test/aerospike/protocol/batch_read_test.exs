defmodule Aerospike.Protocol.BatchReadTest do
  use ExUnit.Case, async: true

  alias Aerospike.BatchCommand.Entry
  alias Aerospike.BatchCommand.NodeRequest
  alias Aerospike.Error
  alias Aerospike.Key
  alias Aerospike.Protocol.AsmMsg
  alias Aerospike.Protocol.AsmMsg.Field
  alias Aerospike.Protocol.AsmMsg.Operation
  alias Aerospike.Protocol.BatchRead
  alias Aerospike.Protocol.Message
  alias Aerospike.Protocol.Response

  describe "encode_request/2" do
    test "emits a batch-index-with-set field and repeat rows for shared namespace/set" do
      key1 = Key.new("test", "users", "k1")
      key2 = Key.new("test", "users", "k2")
      key3 = Key.new("test", "orders", "k3")

      request =
        %NodeRequest{
          node_name: "A1",
          entries: [
            %Entry{index: 0, key: key1, payload: nil},
            %Entry{index: 3, key: key2, payload: nil},
            %Entry{index: 5, key: key3, payload: nil}
          ],
          payload: nil
        }

      encoded = BatchRead.encode_request(request, mode: :all_bins)

      assert {:ok, {2, 3, body}} = Message.decode(IO.iodata_to_binary(encoded))
      assert {:ok, msg} = AsmMsg.decode(body)
      assert msg.info1 == Bitwise.bor(AsmMsg.info1_read(), AsmMsg.info1_batch())
      assert [%Field{type: type, data: data}] = msg.fields
      assert type == Field.type_batch_index_with_set()

      ns_field = Field.encode(Field.namespace("test"))
      users_set_field = Field.encode(Field.set("users"))
      orders_set_field = Field.encode(Field.set("orders"))
      row_attr = Bitwise.bor(AsmMsg.info1_read(), AsmMsg.info1_get_all())

      assert data ==
               <<
                 3::32-big,
                 0::8,
                 0::32-big,
                 key1.digest::binary,
                 0::8,
                 row_attr::8,
                 2::16-big,
                 0::16-big,
                 ns_field::binary,
                 users_set_field::binary,
                 3::32-big,
                 key2.digest::binary,
                 1::8,
                 5::32-big,
                 key3.digest::binary,
                 0::8,
                 row_attr::8,
                 2::16-big,
                 0::16-big,
                 ns_field::binary,
                 orders_set_field::binary
               >>
    end
  end

  describe "parse_batch_read_response/3" do
    test "preserves indexed success and per-record server errors" do
      key1 = Key.new("test", "users", "k1")
      key2 = Key.new("test", "users", "missing")
      key3 = Key.new("test", "users", "stale")

      request =
        %NodeRequest{
          node_name: "A1",
          entries: [
            %Entry{index: 1, key: key1, payload: nil},
            %Entry{index: 4, key: key2, payload: nil},
            %Entry{index: 7, key: key3, payload: nil}
          ],
          payload: nil
        }

      body =
        IO.iodata_to_binary([
          batch_row(1, 0, 3, 120, [
            %Operation{op_type: 1, particle_type: 3, bin_name: "name", data: "Ada"}
          ]),
          batch_row(4, 2, 0, 0, []),
          batch_row(7, 11, 0, 0, []),
          last_row()
        ])

      assert {:ok, reply} = Response.parse_batch_read_response(body, request, mode: :all_bins)

      assert [
               %BatchRead.RecordResult{
                 index: 1,
                 key: ^key1,
                 result: :ok,
                 generation: 3,
                 ttl: 120,
                 bins: %{"name" => "Ada"}
               },
               %BatchRead.RecordResult{
                 index: 4,
                 key: ^key2,
                 result: {:error, %Error{code: :key_not_found}},
                 generation: nil,
                 ttl: nil,
                 bins: nil
               },
               %BatchRead.RecordResult{
                 index: 7,
                 key: ^key3,
                 result: {:error, %Error{code: :partition_unavailable}},
                 generation: nil,
                 ttl: nil,
                 bins: nil
               }
             ] = reply.records
    end

    test "rejects replies that reference an index outside the grouped node request" do
      key = Key.new("test", "users", "k1")

      request =
        %NodeRequest{
          node_name: "A1",
          entries: [%Entry{index: 2, key: key, payload: nil}],
          payload: nil
        }

      body = IO.iodata_to_binary([batch_row(9, 0, 1, 20, []), last_row()])

      assert {:error, %Error{code: :parse_error, message: message}} =
               Response.parse_batch_read_response(body, request, mode: :header)

      assert message =~ "unknown batch index 9"
    end

    test "rejects header-mode replies that send operations" do
      key = Key.new("test", "users", "k1")

      request =
        %NodeRequest{
          node_name: "A1",
          entries: [%Entry{index: 2, key: key, payload: nil}],
          payload: nil
        }

      body =
        IO.iodata_to_binary([
          batch_row(2, 0, 1, 20, [
            %Operation{
              op_type: 1,
              particle_type: 1,
              bin_name: "count",
              data: <<7::64-signed-big>>
            }
          ]),
          last_row()
        ])

      assert {:error, %Error{code: :parse_error, message: message}} =
               Response.parse_batch_read_response(body, request, mode: :header)

      assert message =~ "header-only batch read reply contained 1 operations"
    end
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
end
