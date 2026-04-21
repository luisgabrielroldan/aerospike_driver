defmodule Aerospike.Protocol.AsmMsgTest do
  use ExUnit.Case, async: true

  alias Aerospike.Key
  alias Aerospike.Protocol.AsmMsg
  alias Aerospike.Protocol.AsmMsg.Field
  alias Aerospike.Protocol.AsmMsg.Operation

  @namespace "test"
  @set "spike"

  describe "key_command/3" do
    test "builds a write request with digest, set, namespace, and user key fields" do
      key = Key.new(@namespace, @set, "k1")
      {:ok, write_op} = Operation.write("bin_a", 7)

      msg =
        AsmMsg.key_command(key, [write_op],
          write: true,
          send_key: true,
          timeout: 5_000
        )

      assert msg.info1 == 0
      assert msg.info2 == AsmMsg.info2_write()
      assert msg.timeout == 5_000

      assert Enum.map(msg.fields, & &1.type) == [
               Field.type_namespace(),
               Field.type_table(),
               Field.type_digest(),
               Field.type_key()
             ]

      assert msg.operations == [write_op]
    end

    test "builds a header-only exists-style request without bin data ops" do
      key = Key.new(@namespace, @set, "k1")

      msg = AsmMsg.key_command(key, [], read: true, read_header: true)

      assert msg.info1 == Bitwise.bor(AsmMsg.info1_read(), AsmMsg.info1_nobindata())
      assert msg.info2 == 0
      assert msg.operations == []
    end

    test "builds a mixed operate request with read and write attrs separated" do
      key = Key.new(@namespace, @set, "k1")
      {:ok, write_op} = Operation.write("count", 42)
      read_op = Operation.read("count")

      msg =
        AsmMsg.key_command(key, [write_op, read_op],
          read: true,
          write: true,
          respond_all_ops: true
        )

      assert msg.info1 == AsmMsg.info1_read()
      assert msg.info2 == Bitwise.bor(AsmMsg.info2_write(), AsmMsg.info2_respond_all_ops())
    end

    test "sets the delete flag for record-delete requests" do
      key = Key.new(@namespace, @set, "k1")

      msg = AsmMsg.key_command(key, [], write: true, delete: true)

      assert msg.info2 ==
               Bitwise.bor(AsmMsg.info2_write(), AsmMsg.info2_delete())
    end

    test "sets the generation flag and header when generation equality is requested" do
      key = Key.new(@namespace, @set, "k1")

      msg = AsmMsg.key_command(key, [], write: true, generation: 7)

      assert msg.info2 ==
               Bitwise.bor(AsmMsg.info2_write(), AsmMsg.info2_generation())

      assert msg.generation == 7
    end

    test "does not set the generation flag for the default generation zero" do
      key = Key.new(@namespace, @set, "k1")

      msg = AsmMsg.key_command(key, [], write: true, generation: 0)

      assert msg.info2 == AsmMsg.info2_write()
      assert msg.generation == 0
    end
  end
end
