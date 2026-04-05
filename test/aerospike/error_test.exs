defmodule Aerospike.ErrorTest do
  use ExUnit.Case, async: true

  alias Aerospike.Error

  doctest Error

  describe "from_result_code/2" do
    test "builds struct with ResultCode message" do
      e = Error.from_result_code(:key_not_found)
      assert e.code == :key_not_found
      assert is_binary(e.message)
      assert e.node == nil
      assert e.in_doubt == false
    end

    test "accepts overrides" do
      e = Error.from_result_code(:timeout, message: "custom", node: "N1", in_doubt: true)
      assert e.message == "custom"
      assert e.node == "N1"
      assert e.in_doubt == true
    end
  end

  describe "Exception.message/1" do
    test "formats code and message" do
      e = Error.from_result_code(:key_not_found)
      assert Exception.message(e) =~ "key_not_found"
      assert Exception.message(e) =~ e.message
    end
  end

  test "raise and rescue" do
    e = Error.from_result_code(:timeout, message: "boom")

    assert_raise Error, fn ->
      raise e
    end

    try do
      raise e
    rescue
      err in [Error] ->
        assert err.code == :timeout
        assert err.message == "boom"
    end
  end
end
