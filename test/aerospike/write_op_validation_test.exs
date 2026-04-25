defmodule Aerospike.Command.WriteOpValidationTest do
  use ExUnit.Case, async: true

  alias Aerospike.Command.WriteOp
  alias Aerospike.Key

  @key Key.new("test", "write_ops", "validation")

  describe "execute/5 validation" do
    test "rejects empty mutation maps before touching the cluster runtime" do
      assert {:error, %Aerospike.Error{code: :invalid_argument, message: message}} =
               WriteOp.execute(self(), @key, :add, %{}, [])

      assert message == "add requires at least one bin mutation"
    end

    test "rejects non-map mutation inputs before touching the cluster runtime" do
      assert {:error, %Aerospike.Error{code: :invalid_argument, message: message}} =
               WriteOp.execute(self(), @key, :append, [{"name", "x"}], [])

      assert message == "append bins must be a non-empty map, got: [{\"name\", \"x\"}]"
    end

    test "returns operation validation errors without starting a unary command" do
      assert {:error, %Aerospike.Error{code: :invalid_argument, message: message}} =
               WriteOp.execute(self(), @key, :prepend, %{name: 7}, [])

      assert message =~ "prepend requires a binary value"
    end

    test "normalizes binary bin names while validating add payloads" do
      assert {:error, %Aerospike.Error{code: :invalid_argument, message: message}} =
               WriteOp.execute(self(), @key, :add, %{"count" => "not-a-number"}, [])

      assert message =~ "add requires an integer or float value"
    end
  end
end
