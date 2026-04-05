defmodule Aerospike.Protocol.ResultCodeTest do
  use ExUnit.Case, async: true
  doctest Aerospike.Protocol.ResultCode

  alias Aerospike.Protocol.ResultCode

  describe "from_integer/1" do
    test "converts 0 to :ok" do
      assert ResultCode.from_integer(0) == {:ok, :ok}
    end

    test "converts common server error codes" do
      assert ResultCode.from_integer(1) == {:ok, :server_error}
      assert ResultCode.from_integer(2) == {:ok, :key_not_found}
      assert ResultCode.from_integer(3) == {:ok, :generation_error}
      assert ResultCode.from_integer(5) == {:ok, :key_exists}
      assert ResultCode.from_integer(9) == {:ok, :timeout}
    end

    test "converts client-side error codes (negative)" do
      assert ResultCode.from_integer(-1) == {:ok, :serialize_error}
      assert ResultCode.from_integer(-2) == {:ok, :parse_error}
      assert ResultCode.from_integer(-18) == {:ok, :network_error}
      assert ResultCode.from_integer(-19) == {:ok, :no_response}
    end

    test "converts security codes" do
      assert ResultCode.from_integer(51) == {:ok, :security_not_supported}
      assert ResultCode.from_integer(80) == {:ok, :not_authenticated}
      assert ResultCode.from_integer(81) == {:ok, :role_violation}
    end

    test "converts MRT codes" do
      assert ResultCode.from_integer(120) == {:ok, :mrt_blocked}
      assert ResultCode.from_integer(121) == {:ok, :mrt_version_mismatch}
      assert ResultCode.from_integer(122) == {:ok, :mrt_expired}
    end

    test "converts index codes" do
      assert ResultCode.from_integer(200) == {:ok, :index_found}
      assert ResultCode.from_integer(201) == {:ok, :index_not_found}
    end

    test "returns error for unknown codes" do
      assert ResultCode.from_integer(999) == {:error, 999}
      assert ResultCode.from_integer(-999) == {:error, -999}
    end
  end

  describe "to_integer/1" do
    test "converts :ok to 0" do
      assert ResultCode.to_integer(:ok) == {:ok, 0}
    end

    test "converts common error atoms" do
      assert ResultCode.to_integer(:key_not_found) == {:ok, 2}
      assert ResultCode.to_integer(:key_exists) == {:ok, 5}
      assert ResultCode.to_integer(:timeout) == {:ok, 9}
    end

    test "converts client-side error atoms" do
      assert ResultCode.to_integer(:network_error) == {:ok, -18}
      assert ResultCode.to_integer(:parse_error) == {:ok, -2}
    end

    test "returns error for unknown atoms" do
      assert ResultCode.to_integer(:unknown_code) == {:error, :unknown_code}
      assert ResultCode.to_integer(:not_a_real_code) == {:error, :not_a_real_code}
    end
  end

  describe "roundtrip conversion" do
    test "all known codes roundtrip through from_integer and to_integer" do
      for {code, atom} <- ResultCode.codes_map() do
        assert {:ok, ^atom} = ResultCode.from_integer(code)
        assert {:ok, ^code} = ResultCode.to_integer(atom)
      end
    end
  end

  describe "message/1" do
    test "returns human-readable messages for known codes" do
      assert ResultCode.message(:ok) == "Operation succeeded"
      assert ResultCode.message(:key_not_found) == "Key not found"
      assert ResultCode.message(:timeout) == "Timeout"
      assert ResultCode.message(:network_error) == "Network error"
    end

    test "returns default message for unknown codes" do
      assert ResultCode.message(:unknown) == "Unknown error code"
    end

    test "all known codes have non-empty messages" do
      for atom <- ResultCode.all_codes() do
        message = ResultCode.message(atom)
        assert is_binary(message)
        assert String.length(message) > 0
        assert message != "Unknown error code"
      end
    end
  end

  describe "success?/1" do
    test "returns true only for :ok" do
      assert ResultCode.success?(:ok) == true
    end

    test "returns false for error codes" do
      assert ResultCode.success?(:key_not_found) == false
      assert ResultCode.success?(:timeout) == false
      assert ResultCode.success?(:network_error) == false
      assert ResultCode.success?(:server_error) == false
    end
  end

  describe "all_codes/0" do
    test "returns a non-empty list of atoms" do
      codes = ResultCode.all_codes()
      assert is_list(codes)
      assert length(codes) > 70
      assert Enum.all?(codes, &is_atom/1)
    end

    test "includes common codes" do
      codes = ResultCode.all_codes()
      assert :ok in codes
      assert :key_not_found in codes
      assert :timeout in codes
      assert :network_error in codes
    end
  end

  describe "codes_map/0" do
    test "returns a map of integer to atom" do
      map = ResultCode.codes_map()
      assert is_map(map)
      assert map[0] == :ok
      assert map[2] == :key_not_found
      assert map[-18] == :network_error
    end
  end
end
