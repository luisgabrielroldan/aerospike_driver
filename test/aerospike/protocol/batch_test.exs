defmodule Aerospike.Protocol.BatchTest do
  use ExUnit.Case, async: true

  import Bitwise

  alias Aerospike.Command.BatchCommand
  alias Aerospike.Command.BatchCommand.Entry
  alias Aerospike.Command.BatchCommand.NodeRequest
  alias Aerospike.Error
  alias Aerospike.Key
  alias Aerospike.Protocol.AsmMsg
  alias Aerospike.Protocol.AsmMsg.Field
  alias Aerospike.Protocol.AsmMsg.Operation
  alias Aerospike.Protocol.Batch
  alias Aerospike.Protocol.BatchRead
  alias Aerospike.Protocol.Message
  alias Aerospike.Protocol.MessagePack
  alias Aerospike.Protocol.Response

  describe "encode_request/2" do
    test "encodes mixed entry kinds through one batch-index field" do
      key_read = Key.new("test", "users", "k-read")
      key_header = Key.new("test", "users", "k-header")
      key_exists = Key.new("test", "users", "k-exists")
      key_put = Key.new("test", "users", "k-put")
      key_delete = Key.new("test", "users", "k-delete")
      key_operate = Key.new("test", "users", "k-operate")
      key_udf = Key.new("test", "users", "k-udf")

      {:ok, put_op} = Operation.write("name", "Ada")
      {:ok, add_op} = Operation.add("visits", 1)
      read_op = Operation.read("visits")

      request = %NodeRequest{
        node_name: "A1",
        entries: [
          %Entry{
            index: 0,
            key: key_read,
            kind: :read,
            dispatch: {:read, :master, 0},
            payload: nil
          },
          %Entry{
            index: 1,
            key: key_header,
            kind: :read_header,
            dispatch: {:read, :master, 0},
            payload: nil
          },
          %Entry{
            index: 2,
            key: key_exists,
            kind: :exists,
            dispatch: {:read, :master, 0},
            payload: nil
          },
          %Entry{
            index: 3,
            key: key_put,
            kind: :put,
            dispatch: :write,
            payload: %{operations: [put_op], ttl: 60, generation: 7}
          },
          %Entry{
            index: 4,
            key: key_delete,
            kind: :delete,
            dispatch: :write,
            payload: %{durable_delete: true}
          },
          %Entry{
            index: 5,
            key: key_operate,
            kind: :operate,
            dispatch: :write,
            payload: %{operations: [add_op, read_op], ttl: 120, generation: 3}
          },
          %Entry{
            index: 6,
            key: key_udf,
            kind: :udf,
            dispatch: :write,
            payload: %{package: "math", function: "inc", args: [1, "x"], ttl: 15}
          }
        ],
        payload: nil
      }

      encoded = Batch.encode_request(request, timeout: 250)

      assert {:ok, {2, 3, body}} = Message.decode(IO.iodata_to_binary(encoded))
      assert {:ok, msg} = AsmMsg.decode(body)
      assert msg.info1 == AsmMsg.info1_batch()
      assert msg.timeout == 250
      assert [%Field{type: type, data: data}] = msg.fields
      assert type == Field.type_batch_index()

      ns_field = Field.encode(Field.namespace("test"))
      set_field = Field.encode(Field.set("users"))
      put_op_bytes = Operation.encode(put_op)
      add_op_bytes = Operation.encode(add_op)
      read_op_bytes = Operation.encode(read_op)
      udf_args = MessagePack.pack!([1, {:particle_string, "x"}])

      assert data ==
               IO.iodata_to_binary([
                 <<7::32-big, 0::8>>,
                 mixed_row(0, key_read, [
                   <<0x0A::8, bor(AsmMsg.info1_read(), AsmMsg.info1_get_all())::8, 0::8, 0::8,
                     0::32-big>>,
                   <<2::16-big, 0::16-big>>,
                   ns_field,
                   set_field
                 ]),
                 mixed_row(1, key_header, [
                   <<0x0A::8, bor(AsmMsg.info1_read(), AsmMsg.info1_nobindata())::8, 0::8, 0::8,
                     0::32-big>>,
                   <<2::16-big, 0::16-big>>,
                   ns_field,
                   set_field
                 ]),
                 mixed_row(2, key_exists, [
                   <<0x0A::8, bor(AsmMsg.info1_read(), AsmMsg.info1_nobindata())::8, 0::8, 0::8,
                     0::32-big>>,
                   <<2::16-big, 0::16-big>>,
                   ns_field,
                   set_field
                 ]),
                 mixed_row(3, key_put, [
                   <<0x0E::8, 0::8,
                     bor(
                       AsmMsg.info2_write(),
                       bor(AsmMsg.info2_respond_all_ops(), AsmMsg.info2_generation())
                     )::8, 0::8, 7::16-big, 60::32-big>>,
                   <<2::16-big, 1::16-big>>,
                   ns_field,
                   set_field,
                   put_op_bytes
                 ]),
                 mixed_row(4, key_delete, [
                   <<0x0E::8, 0::8,
                     bor(
                       AsmMsg.info2_write(),
                       bor(
                         AsmMsg.info2_delete(),
                         bor(AsmMsg.info2_respond_all_ops(), AsmMsg.info2_durable_delete())
                       )
                     )::8, 0::8, 0::16-big, 0::32-big>>,
                   <<2::16-big, 0::16-big>>,
                   ns_field,
                   set_field
                 ]),
                 mixed_row(5, key_operate, [
                   <<0x0E::8, AsmMsg.info1_read()::8,
                     bor(AsmMsg.info2_write(), AsmMsg.info2_generation())::8, 0::8, 3::16-big,
                     120::32-big>>,
                   <<2::16-big, 2::16-big>>,
                   ns_field,
                   set_field,
                   add_op_bytes,
                   read_op_bytes
                 ]),
                 mixed_row(6, key_udf, [
                   <<0x0E::8, 0::8, AsmMsg.info2_write()::8, 0::8, 0::16-big, 15::32-big>>,
                   <<5::16-big, 0::16-big>>,
                   ns_field,
                   set_field,
                   Field.encode(Field.udf_package_name("math")),
                   Field.encode(Field.udf_function("inc")),
                   Field.encode(Field.udf_arglist(udf_args))
                 ])
               ])
    end

    test "keeps homogeneous batch-read as a wrapper over the broader protocol encoder" do
      key = Key.new("test", "users", "k1")

      request = %NodeRequest{
        node_name: "A1",
        entries: [
          %Entry{index: 0, key: key, kind: :read, dispatch: {:read, :master, 0}, payload: nil}
        ],
        payload: nil
      }

      assert BatchRead.encode_request(request, mode: :all_bins) ==
               Batch.encode_request(request, layout: :batch_index_with_set)
    end

    test "encodes read-only operate entries without write flags" do
      key = Key.new("test", "users", "k-operate-read")
      read_op = Operation.read("name")

      request = %NodeRequest{
        node_name: "A1",
        entries: [
          %Entry{
            index: 0,
            key: key,
            kind: :operate,
            dispatch: {:read, :master, 0},
            payload: %{operations: [read_op]}
          }
        ],
        payload: nil
      }

      encoded = Batch.encode_request(request, timeout: 250)

      assert {:ok, {2, 3, body}} = Message.decode(IO.iodata_to_binary(encoded))
      assert {:ok, msg} = AsmMsg.decode(body)
      assert msg.info1 == AsmMsg.info1_batch()
      assert msg.timeout == 250
      assert [%Field{type: type, data: data}] = msg.fields
      assert type == Field.type_batch_index()

      assert data ==
               IO.iodata_to_binary([
                 <<1::32-big, 0::8>>,
                 mixed_row(0, key, [
                   <<0x0A::8, AsmMsg.info1_read()::8, 0::8, 0::8, 0::32-big>>,
                   <<2::16-big, 1::16-big>>,
                   Field.encode(Field.namespace("test")),
                   Field.encode(Field.set("users")),
                   Operation.encode(read_op)
                 ])
               ])
    end

    test "encodes projected read entries with read operations" do
      key = Key.new("test", "users", "k-read")
      name_op = Operation.read("name")
      count_op = Operation.read("count")

      request = %NodeRequest{
        node_name: "A1",
        entries: [
          %Entry{
            index: 0,
            key: key,
            kind: :read,
            dispatch: {:read, :master, 0},
            payload: %{operations: [name_op, count_op]}
          }
        ],
        payload: nil
      }

      encoded = Batch.encode_request(request, timeout: 250)

      assert {:ok, {2, 3, body}} = Message.decode(IO.iodata_to_binary(encoded))
      assert {:ok, msg} = AsmMsg.decode(body)
      assert msg.info1 == AsmMsg.info1_batch()
      assert msg.timeout == 250
      assert [%Field{type: type, data: data}] = msg.fields
      assert type == Field.type_batch_index()

      assert data ==
               IO.iodata_to_binary([
                 <<1::32-big, 0::8>>,
                 mixed_row(0, key, [
                   <<0x0A::8, AsmMsg.info1_read()::8, 0::8, 0::8, 0::32-big>>,
                   <<2::16-big, 2::16-big>>,
                   Field.encode(Field.namespace("test")),
                   Field.encode(Field.set("users")),
                   Operation.encode(name_op),
                   Operation.encode(count_op)
                 ])
               ])
    end

    test "encodes delete entries with empty payload" do
      key = Key.new("test", "users", "k-delete")

      request = %NodeRequest{
        node_name: "A1",
        entries: [
          %Entry{index: 0, key: key, kind: :delete, dispatch: :write, payload: nil}
        ],
        payload: nil
      }

      encoded = Batch.encode_request(request, timeout: 250)

      assert {:ok, {2, 3, body}} = Message.decode(IO.iodata_to_binary(encoded))
      assert {:ok, msg} = AsmMsg.decode(body)
      assert msg.info1 == AsmMsg.info1_batch()
      assert msg.timeout == 250
      assert [%Field{type: type, data: data}] = msg.fields
      assert type == Field.type_batch_index()

      assert data ==
               IO.iodata_to_binary([
                 <<1::32-big, 0::8>>,
                 mixed_row(0, key, [
                   <<0x0E::8, 0::8,
                     bor(
                       AsmMsg.info2_write(),
                       bor(AsmMsg.info2_delete(), AsmMsg.info2_respond_all_ops())
                     )::8, 0::8, 0::16-big, 0::32-big>>,
                   <<2::16-big, 0::16-big>>,
                   Field.encode(Field.namespace("test")),
                   Field.encode(Field.set("users"))
                 ])
               ])
    end

    test "encodes udf entries with package function and packed arguments" do
      key = Key.new("test", "users", "k-udf")

      request = %NodeRequest{
        node_name: "A1",
        entries: [
          %Entry{
            index: 0,
            key: key,
            kind: :udf,
            dispatch: :write,
            payload: %{package: "demo", function: "echo", args: ["x", 1]}
          }
        ],
        payload: nil
      }

      encoded = Batch.encode_request(request, timeout: 250)

      assert {:ok, {2, 3, body}} = Message.decode(IO.iodata_to_binary(encoded))
      assert {:ok, msg} = AsmMsg.decode(body)
      assert msg.info1 == AsmMsg.info1_batch()
      assert msg.timeout == 250
      assert [%Field{type: type, data: data}] = msg.fields
      assert type == Field.type_batch_index()

      assert data ==
               IO.iodata_to_binary([
                 <<1::32-big, 0::8>>,
                 mixed_row(0, key, [
                   <<0x0E::8, 0::8, AsmMsg.info2_write()::8, 0::8, 0::16-big, 0::32-big>>,
                   <<5::16-big, 0::16-big>>,
                   Field.encode(Field.namespace("test")),
                   Field.encode(Field.set("users")),
                   Field.encode(Field.udf_package_name("demo")),
                   Field.encode(Field.udf_function("echo")),
                   Field.encode(
                     Field.udf_arglist(MessagePack.pack!([{:particle_string, "x"}, 1]))
                   )
                 ])
               ])
    end
  end

  describe "parse_response/2" do
    test "parses mixed per-entry results with stable kinds and records" do
      key_read = Key.new("test", "users", "k-read")
      key_header = Key.new("test", "users", "k-header")
      key_exists = Key.new("test", "users", "k-exists")
      key_put = Key.new("test", "users", "k-put")
      key_delete = Key.new("test", "users", "k-delete")
      key_operate = Key.new("test", "users", "k-operate")
      key_udf = Key.new("test", "users", "k-udf")

      request = %NodeRequest{
        node_name: "A1",
        entries: [
          %Entry{
            index: 0,
            key: key_read,
            kind: :read,
            dispatch: {:read, :master, 0},
            payload: nil
          },
          %Entry{
            index: 1,
            key: key_header,
            kind: :read_header,
            dispatch: {:read, :master, 0},
            payload: nil
          },
          %Entry{
            index: 2,
            key: key_exists,
            kind: :exists,
            dispatch: {:read, :master, 0},
            payload: nil
          },
          %Entry{index: 3, key: key_put, kind: :put, dispatch: :write, payload: nil},
          %Entry{index: 4, key: key_delete, kind: :delete, dispatch: :write, payload: nil},
          %Entry{index: 5, key: key_operate, kind: :operate, dispatch: :write, payload: nil},
          %Entry{index: 6, key: key_udf, kind: :udf, dispatch: :write, payload: nil}
        ],
        payload: nil
      }

      body =
        IO.iodata_to_binary([
          batch_row(0, 0, 3, 120, [
            %Operation{op_type: 1, particle_type: 3, bin_name: "name", data: "Ada"}
          ]),
          batch_row(1, 0, 7, 90, []),
          batch_row(2, 2, 0, 0, []),
          batch_row(3, 0, 0, 0, []),
          batch_row(4, 0, 0, 0, []),
          batch_row(5, 0, 8, 240, [
            %Operation{
              op_type: 1,
              particle_type: 1,
              bin_name: "count",
              data: <<7::64-signed-big>>
            }
          ]),
          batch_row(6, 0, 11, 60, [
            %Operation{op_type: 1, particle_type: 3, bin_name: "result", data: "ok"}
          ]),
          last_row()
        ])

      assert {:ok, %Batch.Reply{results: results}} = Response.parse_batch_response(body, request)

      assert [
               %BatchCommand.Result{
                 index: 0,
                 key: ^key_read,
                 kind: :read,
                 status: :ok,
                 record: %{bins: %{"name" => "Ada"}, generation: 3, ttl: 120},
                 error: nil
               },
               %BatchCommand.Result{
                 index: 1,
                 key: ^key_header,
                 kind: :read_header,
                 status: :ok,
                 record: %{generation: 7, ttl: 90},
                 error: nil
               },
               %BatchCommand.Result{
                 index: 2,
                 key: ^key_exists,
                 kind: :exists,
                 status: :error,
                 record: nil,
                 error: %Error{code: :key_not_found}
               },
               %BatchCommand.Result{
                 index: 3,
                 key: ^key_put,
                 kind: :put,
                 status: :ok,
                 record: nil,
                 error: nil
               },
               %BatchCommand.Result{
                 index: 4,
                 key: ^key_delete,
                 kind: :delete,
                 status: :ok,
                 record: nil,
                 error: nil
               },
               %BatchCommand.Result{
                 index: 5,
                 key: ^key_operate,
                 kind: :operate,
                 status: :ok,
                 record: %{bins: %{"count" => 7}, generation: 8, ttl: 240},
                 error: nil
               },
               %BatchCommand.Result{
                 index: 6,
                 key: ^key_udf,
                 kind: :udf,
                 status: :ok,
                 record: %{bins: %{"result" => "ok"}, generation: 11, ttl: 60},
                 error: nil
               }
             ] = results
    end

    test "rejects replies that reference an index outside the grouped node request" do
      key = Key.new("test", "users", "k1")

      request = %NodeRequest{
        node_name: "A1",
        entries: [
          %Entry{index: 2, key: key, kind: :read, dispatch: {:read, :master, 0}, payload: nil}
        ],
        payload: nil
      }

      body = IO.iodata_to_binary([batch_row(9, 0, 1, 20, []), last_row()])

      assert {:error, %Error{code: :parse_error, message: message}} =
               Batch.parse_response(body, request)

      assert message =~ "unknown batch index 9"
    end

    test "returns only the indices present in a partial node reply" do
      key_a = Key.new("test", "users", "k-a")
      key_b = Key.new("test", "users", "k-b")

      request = %NodeRequest{
        node_name: "A1",
        entries: [
          %Entry{index: 0, key: key_a, kind: :read, dispatch: {:read, :master, 0}, payload: nil},
          %Entry{index: 1, key: key_b, kind: :read, dispatch: {:read, :master, 0}, payload: nil}
        ],
        payload: nil
      }

      body =
        IO.iodata_to_binary([
          batch_row(1, 0, 5, 33, [
            %Operation{op_type: 1, particle_type: 3, bin_name: "name", data: "Bert"}
          ]),
          last_row()
        ])

      assert {:ok, %Batch.Reply{results: [%BatchCommand.Result{index: 1, key: ^key_b}]}} =
               Batch.parse_response(body, request)
    end

    test "parses udf error rows as per-entry failures" do
      key = Key.new("test", "users", "k-udf")

      request = %NodeRequest{
        node_name: "A1",
        entries: [
          %Entry{index: 0, key: key, kind: :udf, dispatch: :write, payload: nil}
        ],
        payload: nil
      }

      body = IO.iodata_to_binary([batch_row(0, 100, 0, 0, []), last_row()])

      assert {:ok, %Batch.Reply{results: [result]}} = Response.parse_batch_response(body, request)

      assert %BatchCommand.Result{
               index: 0,
               key: ^key,
               kind: :udf,
               status: :error,
               record: nil,
               error: %Error{code: :udf_bad_response}
             } = result
    end

    test "accepts successful put and delete replies that include echoed operations" do
      key_put = Key.new("test", "users", "k-put")
      key_delete = Key.new("test", "users", "k-delete")

      request = %NodeRequest{
        node_name: "A1",
        entries: [
          %Entry{index: 0, key: key_put, kind: :put, dispatch: :write, payload: nil},
          %Entry{index: 1, key: key_delete, kind: :delete, dispatch: :write, payload: nil}
        ],
        payload: nil
      }

      body =
        IO.iodata_to_binary([
          batch_row(0, 0, 3, 120, [
            %Operation{op_type: 1, particle_type: 3, bin_name: "name", data: "Ada"}
          ]),
          batch_row(1, 0, 0, 0, [
            %Operation{op_type: 1, particle_type: 3, bin_name: "name", data: "gone"}
          ]),
          last_row()
        ])

      assert {:ok, %Batch.Reply{results: results}} = Response.parse_batch_response(body, request)

      assert [
               %BatchCommand.Result{
                 index: 0,
                 key: ^key_put,
                 kind: :put,
                 status: :ok,
                 record: nil,
                 error: nil
               },
               %BatchCommand.Result{
                 index: 1,
                 key: ^key_delete,
                 kind: :delete,
                 status: :ok,
                 record: nil,
                 error: nil
               }
             ] = results
    end
  end

  defp mixed_row(index, key, row_body) do
    [<<index::32-big, key.digest::binary>>, row_body]
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
