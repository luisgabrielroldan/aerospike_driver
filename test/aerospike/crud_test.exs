defmodule Aerospike.CRUDTest do
  use ExUnit.Case, async: true

  alias Aerospike.CRUD
  alias Aerospike.Key
  alias Aerospike.Protocol.AsmMsg
  alias Aerospike.Protocol.Message
  alias Aerospike.Test.Helpers

  setup do
    key = Key.new("test", "users", "wire-test-key")
    {:ok, key: key}
  end

  describe "normalize_bins/1" do
    test "converts atom keys to strings" do
      assert CRUD.normalize_bins(%{a: 1, b: 2}) == %{"a" => 1, "b" => 2}
    end

    test "keeps string keys" do
      assert CRUD.normalize_bins(%{"x" => 1}) == %{"x" => 1}
    end

    test "raises on invalid key type" do
      assert_raise ArgumentError, fn -> CRUD.normalize_bins(%{1 => :x}) end
    end
  end

  describe "wire encoding (protocol shape)" do
    test "put message decodes to write with bins", %{key: key} do
      wire = Helpers.put_wire(key, %{"n" => 1, "s" => "x"})
      assert {:ok, {2, 3, body}} = Message.decode(wire)
      assert {:ok, msg} = AsmMsg.decode(body)
      assert msg.info2 == AsmMsg.info2_write()
      assert length(msg.operations) == 2
    end

    test "get message decodes to read", %{key: key} do
      wire = Helpers.get_wire(key)
      assert {:ok, {2, 3, body}} = Message.decode(wire)
      assert {:ok, msg} = AsmMsg.decode(body)
      import Bitwise
      assert msg.info1 == (AsmMsg.info1_read() ||| AsmMsg.info1_get_all())
      assert msg.operations == []
    end

    test "delete message decodes to delete", %{key: key} do
      wire = Helpers.delete_wire(key)
      assert {:ok, {2, 3, body}} = Message.decode(wire)
      assert {:ok, msg} = AsmMsg.decode(body)
      import Bitwise
      assert msg.info2 == (AsmMsg.info2_write() ||| AsmMsg.info2_delete())
    end

    test "exists message decodes to exists", %{key: key} do
      wire = Helpers.exists_wire(key)
      assert {:ok, {2, 3, body}} = Message.decode(wire)
      assert {:ok, msg} = AsmMsg.decode(body)
      import Bitwise
      assert msg.info1 == (AsmMsg.info1_read() ||| AsmMsg.info1_nobindata())
    end

    test "touch message decodes to touch op", %{key: key} do
      wire = Helpers.touch_wire(key)
      assert {:ok, {2, 3, body}} = Message.decode(wire)
      assert {:ok, msg} = AsmMsg.decode(body)
      assert length(msg.operations) == 1
    end
  end
end
