defmodule Aerospike.PolicyBreakerTest do
  use ExUnit.Case, async: true

  alias Aerospike.Policy

  describe "validate_start/1 breaker options" do
    test "default values pass validation" do
      assert {:ok, validated} = Policy.validate_start(name: :test, hosts: ["127.0.0.1:3000"])
      assert validated[:max_error_rate] == 100
      assert validated[:error_rate_window] == 1
    end

    test "max_error_rate 0 passes validation" do
      opts = [name: :test, hosts: ["127.0.0.1:3000"], max_error_rate: 0, error_rate_window: 100]
      assert {:ok, _validated} = Policy.validate_start(opts)
    end

    test "max_error_rate 50 and error_rate_window 1 passes validation" do
      opts = [name: :test, hosts: ["127.0.0.1:3000"], max_error_rate: 50, error_rate_window: 1]
      assert {:ok, _validated} = Policy.validate_start(opts)
    end

    test "max_error_rate 1 and error_rate_window 100 fails validation" do
      opts = [name: :test, hosts: ["127.0.0.1:3000"], max_error_rate: 1, error_rate_window: 100]

      assert {:error, %NimbleOptions.ValidationError{key: :max_error_rate}} =
               Policy.validate_start(opts)
    end

    test "max_error_rate 101 and error_rate_window 1 fails validation" do
      opts = [name: :test, hosts: ["127.0.0.1:3000"], max_error_rate: 101, error_rate_window: 1]

      assert {:error, %NimbleOptions.ValidationError{key: :max_error_rate}} =
               Policy.validate_start(opts)
    end

    test "error_rate_window 0 fails pos_integer validation" do
      opts = [name: :test, hosts: ["127.0.0.1:3000"], error_rate_window: 0]

      assert {:error, %NimbleOptions.ValidationError{key: :error_rate_window}} =
               Policy.validate_start(opts)
    end
  end
end
