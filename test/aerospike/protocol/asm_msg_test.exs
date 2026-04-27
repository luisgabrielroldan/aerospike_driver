defmodule Aerospike.Protocol.AsmMsgTest do
  use ExUnit.Case, async: true

  import Bitwise

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

      msg =
        AsmMsg.key_command(key, [],
          write: true,
          generation: 7,
          generation_policy: :expect_equal
        )

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

    test "sets expanded read policy flags and read-touch TTL" do
      key = Key.new(@namespace, @set, "k1")

      msg =
        AsmMsg.key_command(key, [],
          read: true,
          read_mode_ap: :all,
          read_mode_sc: :allow_unavailable,
          read_touch_ttl_percent: -1,
          use_compression: true
        )

      assert msg.info1 ==
               (AsmMsg.info1_read() ||| AsmMsg.info1_read_mode_ap_all() |||
                  AsmMsg.info1_compress_response())

      assert msg.info3 == (AsmMsg.info3_sc_read_type() ||| AsmMsg.info3_sc_read_relax())
      assert msg.expiration == -1
    end

    test "sets expanded write policy flags and TTL sentinels" do
      key = Key.new(@namespace, @set, "k1")

      msg =
        AsmMsg.key_command(key, [],
          write: true,
          ttl: -2,
          generation: 8,
          generation_policy: :expect_gt,
          commit_level: :master
        )

      assert msg.info2 == (AsmMsg.info2_write() ||| AsmMsg.info2_generation_gt())
      assert msg.info3 == AsmMsg.info3_commit_master()
      assert msg.generation == 8
      assert msg.expiration == -2
    end
  end
end
