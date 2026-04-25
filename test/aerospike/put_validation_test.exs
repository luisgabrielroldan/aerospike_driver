defmodule Aerospike.Command.PutValidationTest do
  use ExUnit.Case, async: true

  alias Aerospike.Command.Put
  alias Aerospike.Key

  @key Key.new("test", "put_ops", "validation")

  describe "execute/4 validation" do
    test "rejects empty bin maps before touching the cluster runtime" do
      assert {:error, %Aerospike.Error{code: :invalid_argument, message: message}} =
               Put.execute(self(), @key, %{}, [])

      assert message == "PUT requires at least one bin write"
    end

    test "rejects non-map bin inputs before touching the cluster runtime" do
      assert {:error, %Aerospike.Error{code: :invalid_argument, message: message}} =
               Put.execute(self(), @key, [{"name", "Ada"}], [])

      assert message == "PUT bins must be a non-empty map, got: [{\"name\", \"Ada\"}]"
    end

    test "returns operation validation errors for unsupported values" do
      assert {:error, %Aerospike.Error{code: :invalid_argument, message: message}} =
               Put.execute(self(), @key, %{"payload" => self()}, [])

      assert message =~ "unsupported write particle"
    end
  end
end
