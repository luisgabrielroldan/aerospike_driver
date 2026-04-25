defmodule Aerospike.Protocol.ResultCodeTest do
  use ExUnit.Case, async: true

  alias Aerospike.Protocol.ResultCode

  test "converts between integer and atom codes" do
    assert ResultCode.from_integer(-18) == {:ok, :network_error}
    assert ResultCode.from_integer(99_999) == {:error, 99_999}

    assert ResultCode.to_integer(:ok) == {:ok, 0}
    assert ResultCode.to_integer(:key_not_found) == {:ok, 2}
    assert ResultCode.to_integer(:not_a_code) == {:error, :not_a_code}
  end

  test "returns messages, success flags, and exported mappings" do
    assert ResultCode.message(:ok) == "Operation succeeded"
    assert ResultCode.message(:unknown) == "Unknown error code"

    assert ResultCode.success?(:ok)
    refute ResultCode.success?(:timeout)

    assert :ok in ResultCode.all_codes()
    assert ResultCode.codes_map()[66] == :expired_session
  end
end
