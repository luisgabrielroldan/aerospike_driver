defmodule Aerospike.PolicyTest do
  use ExUnit.Case, async: true

  alias Aerospike.Error
  alias Aerospike.Policy
  alias Aerospike.RetryPolicy

  describe "cluster_defaults/1" do
    test "delegates cluster retry defaults through the policy seam" do
      assert %Policy.ClusterDefaults{
               retry: %{max_retries: 4, sleep_between_retries_ms: 25, replica_policy: :master}
             } =
               Policy.cluster_defaults(
                 max_retries: 4,
                 sleep_between_retries_ms: 25,
                 replica_policy: :master
               )
    end
  end

  describe "unary_read/2" do
    test "merges retry settings and applies the default timeout" do
      base_retry = RetryPolicy.defaults()

      assert {:ok,
              %Policy.UnaryRead{
                timeout: 5_000,
                retry: %{max_retries: 1, sleep_between_retries_ms: 0, replica_policy: :master}
              }} =
               Policy.unary_read(base_retry, max_retries: 1, replica_policy: :master)
    end

    test "rejects invalid timeout values" do
      assert {:error, %Error{code: :invalid_argument, message: message}} =
               Policy.unary_read(RetryPolicy.defaults(), timeout: -1)

      assert message =~ "timeout must be a non-negative integer"
    end
  end

  describe "unary_write/2" do
    test "materializes timeout, retry, ttl, and generation together" do
      base_retry = RetryPolicy.defaults()

      assert {:ok,
              %Policy.UnaryWrite{
                timeout: 1_500,
                retry: %{max_retries: 0, sleep_between_retries_ms: 0, replica_policy: :sequence},
                ttl: 120,
                generation: 3
              }} =
               Policy.unary_write(base_retry,
                 timeout: 1_500,
                 max_retries: 0,
                 ttl: 120,
                 generation: 3
               )
    end

    test "rejects invalid ttl and generation values" do
      assert {:error, %Error{code: :invalid_argument, message: ttl_message}} =
               Policy.unary_write(RetryPolicy.defaults(), ttl: -1)

      assert ttl_message =~ "ttl must be a non-negative integer"

      assert {:error, %Error{code: :invalid_argument, message: generation_message}} =
               Policy.unary_write(RetryPolicy.defaults(), generation: -1)

      assert generation_message =~ "generation must be a non-negative integer"
    end
  end

  describe "unary_operate/2" do
    test "shares the unary write policy surface" do
      assert {:ok, %Policy.UnaryWrite{ttl: 10, generation: 2}} =
               Policy.unary_operate(RetryPolicy.defaults(), ttl: 10, generation: 2)
    end
  end

  describe "batch_read/2" do
    test "forces batch reads onto no-retry policy while preserving replica selection" do
      base_retry = %{max_retries: 5, sleep_between_retries_ms: 99, replica_policy: :master}

      assert {:ok,
              %Policy.BatchRead{
                timeout: 250,
                retry: %{max_retries: 0, sleep_between_retries_ms: 0, replica_policy: :master}
              }} = Policy.batch_read(base_retry, timeout: 250)
    end

    test "rejects unsupported batch opts" do
      assert {:error, %Error{code: :invalid_argument, message: message}} =
               Policy.batch_read(RetryPolicy.defaults(), max_retries: 1)

      assert message =~ "supports only the :timeout option"
    end
  end

  describe "batch/2" do
    test "materializes the shared batch runtime policy without widening option support" do
      base_retry = %{max_retries: 5, sleep_between_retries_ms: 99, replica_policy: :sequence}

      assert {:ok,
              %Policy.Batch{
                timeout: 250,
                retry: %{max_retries: 0, sleep_between_retries_ms: 0, replica_policy: :sequence}
              }} = Policy.batch(base_retry, timeout: 250)
    end

    test "rejects unsupported batch opts" do
      assert {:error, %Error{code: :invalid_argument, message: message}} =
               Policy.batch(RetryPolicy.defaults(), ttl: 10)

      assert message =~ "supports only the :timeout option"
    end
  end

  describe "admin_info/1" do
    test "defaults the pool checkout timeout" do
      assert {:ok, %Policy.AdminInfo{pool_checkout_timeout: 5_000}} = Policy.admin_info([])
    end

    test "exposes the validated checkout timeout" do
      assert {:ok, policy} = Policy.admin_info(pool_checkout_timeout: 250)
      assert Policy.admin_checkout_timeout(policy) == 250
    end
  end

  describe "scan_query_runtime/1" do
    test "materializes the tracker runtime defaults" do
      assert {:ok, %Policy.ScanQueryRuntime{} = runtime} = Policy.scan_query_runtime([])

      assert runtime.timeout == 5_000
      assert runtime.task_timeout == :infinity
      assert runtime.pool_checkout_timeout == 5_000
      assert runtime.max_concurrent_nodes == 0
      assert runtime.cursor == nil
      assert is_integer(runtime.task_id)
      assert runtime.task_id > 0
    end

    test "rejects invalid task runtime values" do
      assert {:error, %Error{code: :invalid_argument, message: timeout_message}} =
               Policy.scan_query_runtime(task_timeout: :soon)

      assert timeout_message =~ ":task_timeout must be a non-negative integer or :infinity"

      assert {:error, %Error{code: :invalid_argument, message: task_id_message}} =
               Policy.scan_query_runtime(task_id: 0)

      assert task_id_message =~ ":task_id must be a positive integer"
    end
  end
end
