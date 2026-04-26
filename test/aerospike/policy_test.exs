defmodule Aerospike.PolicyTest do
  use ExUnit.Case, async: true

  alias Aerospike.Error
  alias Aerospike.Exp
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
      filter = Exp.eq(Exp.int_bin("age"), Exp.val(18))

      assert {:ok,
              %Policy.UnaryRead{
                timeout: 5_000,
                retry: %{max_retries: 1, sleep_between_retries_ms: 0, replica_policy: :master},
                filter: ^filter
              }} =
               Policy.unary_read(base_retry,
                 max_retries: 1,
                 replica_policy: :master,
                 filter: filter
               )
    end

    test "rejects invalid timeout values" do
      assert {:error, %Error{code: :invalid_argument, message: message}} =
               Policy.unary_read(RetryPolicy.defaults(), timeout: -1)

      assert message =~ "timeout must be a non-negative integer"
    end

    test "rejects malformed filters" do
      assert {:error, %Error{code: :invalid_argument, message: empty_message}} =
               Policy.unary_read(RetryPolicy.defaults(), filter: Exp.from_wire(""))

      assert empty_message =~ "%Aerospike.Exp{}"
      assert empty_message =~ "non-empty wire bytes"

      assert {:error, %Error{code: :invalid_argument, message: bad_message}} =
               Policy.unary_read(RetryPolicy.defaults(), filter: %{wire: <<1>>})

      assert bad_message =~ "%Aerospike.Exp{}"
    end
  end

  describe "unary_write/2" do
    test "materializes timeout, retry, ttl, and generation together" do
      base_retry = RetryPolicy.defaults()
      filter = Exp.gt(Exp.int_bin("count"), Exp.val(0))

      assert {:ok,
              %Policy.UnaryWrite{
                timeout: 1_500,
                retry: %{max_retries: 0, sleep_between_retries_ms: 0, replica_policy: :sequence},
                ttl: 120,
                generation: 3,
                filter: ^filter
              }} =
               Policy.unary_write(base_retry,
                 timeout: 1_500,
                 max_retries: 0,
                 ttl: 120,
                 generation: 3,
                 filter: filter
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

    test "keeps nil filter equivalent to no filter" do
      assert {:ok, %Policy.UnaryWrite{filter: nil}} =
               Policy.unary_write(RetryPolicy.defaults(), filter: nil)
    end
  end

  describe "unary_operate/2" do
    test "shares the unary write policy surface" do
      filter = Exp.eq(Exp.str_bin("status"), Exp.val("active"))

      assert {:ok, %Policy.UnaryWrite{ttl: 10, generation: 2, filter: ^filter}} =
               Policy.unary_operate(RetryPolicy.defaults(),
                 ttl: 10,
                 generation: 2,
                 filter: filter
               )
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

      assert message =~ "support only the :timeout option"
    end

    test "does not accept expression filters before batch wire support exists" do
      assert {:error, %Error{code: :invalid_argument, message: message}} =
               Policy.batch_read(RetryPolicy.defaults(),
                 filter: Exp.eq(Exp.int_bin("a"), Exp.val(1))
               )

      assert message =~ "support only the :timeout option"
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

      assert message =~ "support only the :timeout option"
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

  describe "expression_index_create/2" do
    test "materializes valid expression index options" do
      expression = Exp.int_bin("age")

      assert {:ok,
              %Policy.ExpressionIndexCreate{
                name: "age_expr_idx",
                type: :geo2dsphere,
                collection: :mapvalues,
                pool_checkout_timeout: 250
              }} =
               Policy.expression_index_create(expression,
                 name: "age_expr_idx",
                 type: :geo2dsphere,
                 collection: :mapvalues,
                 pool_checkout_timeout: 250
               )
    end

    test "defaults expression index optional values" do
      assert {:ok,
              %Policy.ExpressionIndexCreate{
                collection: nil,
                pool_checkout_timeout: 5_000
              }} =
               Policy.expression_index_create(Exp.int_bin("age"),
                 name: "age_expr_idx",
                 type: :numeric
               )
    end

    test "rejects missing expression index name" do
      assert {:error, %Error{code: :invalid_argument, message: message}} =
               Policy.expression_index_create(Exp.int_bin("age"), type: :numeric)

      assert message =~ "expression-backed indexes require option :name"
    end

    test "rejects invalid expression index type" do
      assert {:error, %Error{code: :invalid_argument, message: message}} =
               Policy.expression_index_create(Exp.int_bin("age"),
                 name: "age_expr_idx",
                 type: :boolean
               )

      assert message =~ "expression-backed index type"
      assert message =~ ":boolean"
    end

    test "rejects bin-backed index options" do
      assert {:error, %Error{code: :invalid_argument, message: message}} =
               Policy.expression_index_create(Exp.int_bin("age"),
                 bin: "age",
                 name: "age_expr_idx",
                 type: :numeric
               )

      assert message =~ "expression-backed indexes support only"
      assert message =~ ":bin"
    end

    test "rejects empty expression wire" do
      assert {:error, %Error{code: :invalid_argument, message: message}} =
               Policy.expression_index_create(Exp.from_wire(""),
                 name: "age_expr_idx",
                 type: :numeric
               )

      assert message =~ "%Aerospike.Exp{}"
      assert message =~ "non-empty wire bytes"
    end
  end

  describe "xdr_filter/3" do
    test "accepts nil and Aerospike.Exp filters" do
      assert :ok = Policy.xdr_filter("dc-west", "test", nil)

      assert :ok =
               Policy.xdr_filter("dc-west", "test", Exp.eq(Exp.int_bin("age"), Exp.int(21)))
    end

    test "rejects empty or delimiter-bearing identifiers" do
      assert {:error, %Error{code: :invalid_argument, message: datacenter_message}} =
               Policy.xdr_filter("", "test", nil)

      assert datacenter_message =~ "datacenter must be a non-empty string"

      assert {:error, %Error{code: :invalid_argument, message: namespace_message}} =
               Policy.xdr_filter("dc-west", "test;users", nil)

      assert namespace_message =~ "namespace cannot contain info-command delimiters"
    end

    test "rejects empty expression wire and non-expression filters" do
      assert {:error, %Error{code: :invalid_argument, message: empty_message}} =
               Policy.xdr_filter("dc-west", "test", Exp.from_wire(""))

      assert empty_message =~ "XDR filters require nil or a %Aerospike.Exp{}"
      assert empty_message =~ "non-empty wire bytes"

      assert {:error, %Error{code: :invalid_argument, message: filter_message}} =
               Policy.xdr_filter("dc-west", "test", :invalid)

      assert filter_message =~ "XDR filters require nil or a %Aerospike.Exp{}"
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


