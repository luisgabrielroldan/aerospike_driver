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
                socket_timeout: 0,
                retry: %{max_retries: 1, sleep_between_retries_ms: 0, replica_policy: :master},
                filter: ^filter,
                read_mode_ap: :one,
                read_mode_sc: :session,
                read_touch_ttl_percent: 0,
                send_key: false,
                use_compression: nil
              }} =
               Policy.unary_read(base_retry,
                 max_retries: 1,
                 replica_policy: :master,
                 filter: filter
               )
    end

    test "normalizes socket timeout separately from total timeout" do
      assert {:ok, %Policy.UnaryRead{timeout: 1_000, socket_timeout: 250}} =
               Policy.unary_read(RetryPolicy.defaults(), timeout: 1_000, socket_timeout: 250)
    end

    test "normalizes expanded read policy fields" do
      assert {:ok,
              %Policy.UnaryRead{
                read_mode_ap: :all,
                read_mode_sc: :allow_unavailable,
                read_touch_ttl_percent: -1,
                send_key: true,
                use_compression: true
              }} =
               Policy.unary_read(RetryPolicy.defaults(),
                 read_mode_ap: :all,
                 read_mode_sc: :allow_unavailable,
                 read_touch_ttl_percent: -1,
                 send_key: true,
                 use_compression: true
               )
    end

    test "rejects invalid timeout values" do
      assert {:error, %Error{code: :invalid_argument, message: message}} =
               Policy.unary_read(RetryPolicy.defaults(), timeout: -1)

      assert message =~ "timeout must be a non-negative integer"

      assert {:error, %Error{code: :invalid_argument, message: socket_message}} =
               Policy.unary_read(RetryPolicy.defaults(), socket_timeout: -1)

      assert socket_message =~ "socket_timeout must be a non-negative integer"
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

    test "rejects invalid expanded read fields" do
      assert {:error, %Error{message: ap_message}} =
               Policy.unary_read(RetryPolicy.defaults(), read_mode_ap: :quorum)

      assert ap_message =~ "AP read mode"

      assert {:error, %Error{message: sc_message}} =
               Policy.unary_read(RetryPolicy.defaults(), read_mode_sc: :strict)

      assert sc_message =~ "SC read mode"

      assert {:error, %Error{message: touch_message}} =
               Policy.unary_read(RetryPolicy.defaults(), read_touch_ttl_percent: 101)

      assert touch_message =~ "read_touch_ttl_percent"
    end
  end

  describe "unary_write/2" do
    test "materializes timeout, retry, ttl, and generation together" do
      base_retry = RetryPolicy.defaults()
      filter = Exp.gt(Exp.int_bin("count"), Exp.val(0))

      assert {:ok,
              %Policy.UnaryWrite{
                timeout: 1_500,
                socket_timeout: 250,
                retry: %{max_retries: 0, sleep_between_retries_ms: 0, replica_policy: :sequence},
                ttl: 120,
                generation: 3,
                generation_policy: :expect_equal,
                filter: ^filter,
                exists: :replace_only,
                commit_level: :all,
                durable_delete: true,
                respond_per_op: false,
                send_key: true,
                use_compression: nil
              }} =
               Policy.unary_write(base_retry,
                 timeout: 1_500,
                 socket_timeout: 250,
                 max_retries: 0,
                 ttl: 120,
                 generation: 3,
                 exists: :replace_only,
                 durable_delete: true,
                 filter: filter
               )
    end

    test "defaults and validates record-exists write policy values" do
      assert {:ok, %Policy.UnaryWrite{exists: :update}} =
               Policy.unary_write(RetryPolicy.defaults(), [])

      for action <- [:update, :update_only, :create_or_replace, :replace_only, :create_only] do
        assert {:ok, %Policy.UnaryWrite{exists: ^action}} =
                 Policy.unary_write(RetryPolicy.defaults(), exists: action)
      end

      assert {:error, %Error{code: :invalid_argument, message: message}} =
               Policy.unary_write(RetryPolicy.defaults(), exists: :replace)

      assert message =~ "record-exists write policy"
      assert message =~ ":replace"
    end

    test "normalizes expanded write policy fields" do
      assert {:ok,
              %Policy.UnaryWrite{
                ttl: -2,
                generation: 5,
                generation_policy: :expect_gt,
                commit_level: :master,
                respond_per_op: true,
                send_key: false,
                use_compression: false
              }} =
               Policy.unary_write(RetryPolicy.defaults(),
                 ttl: :dont_update,
                 generation: 5,
                 generation_policy: :expect_gt,
                 commit_level: :master,
                 respond_per_op: true,
                 send_key: false,
                 use_compression: false
               )

      assert {:ok, %Policy.UnaryWrite{ttl: -1}} =
               Policy.unary_write(RetryPolicy.defaults(), ttl: :never_expire)

      assert {:ok, %Policy.UnaryWrite{ttl: 0}} =
               Policy.unary_write(RetryPolicy.defaults(), ttl: :default)

      assert {:ok, %Policy.UnaryWrite{generation_policy: :none, generation: 7}} =
               Policy.unary_write(RetryPolicy.defaults(), generation: 7, generation_policy: :none)
    end

    test "defaults and validates durable delete values" do
      assert {:ok, %Policy.UnaryWrite{durable_delete: false}} =
               Policy.unary_write(RetryPolicy.defaults(), [])

      assert {:ok, %Policy.UnaryWrite{durable_delete: true}} =
               Policy.unary_write(RetryPolicy.defaults(), durable_delete: true)

      assert {:error, %Error{code: :invalid_argument, message: message}} =
               Policy.unary_write(RetryPolicy.defaults(), durable_delete: :yes)

      assert message =~ "durable_delete must be a boolean"
    end

    test "rejects invalid expanded write fields" do
      assert {:error, %Error{message: ttl_message}} =
               Policy.unary_write(RetryPolicy.defaults(), ttl: :forever)

      assert ttl_message =~ ":ttl"

      assert {:error, %Error{message: generation_message}} =
               Policy.unary_write(RetryPolicy.defaults(), generation_policy: :greater)

      assert generation_message =~ "generation write policy"

      assert {:error, %Error{message: commit_message}} =
               Policy.unary_write(RetryPolicy.defaults(), commit_level: :replica)

      assert commit_message =~ "commit level"
    end

    test "rejects invalid ttl and generation values" do
      assert {:error, %Error{code: :invalid_argument, message: ttl_message}} =
               Policy.unary_write(RetryPolicy.defaults(), ttl: -1)

      assert ttl_message =~ "ttl must be a non-negative integer"

      assert {:error, %Error{code: :invalid_argument, message: generation_message}} =
               Policy.unary_write(RetryPolicy.defaults(), generation: -1)

      assert generation_message =~ "generation must be a non-negative integer"

      assert {:error, %Error{code: :invalid_argument, message: socket_message}} =
               Policy.unary_write(RetryPolicy.defaults(), socket_timeout: -1)

      assert socket_message =~ "socket_timeout must be a non-negative integer"
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
                socket_timeout: 100,
                max_concurrent_nodes: 0,
                allow_partial_results: true,
                respond_all_keys: true,
                allow_inline: true,
                allow_inline_ssd: false,
                retry: %{max_retries: 0, sleep_between_retries_ms: 0, replica_policy: :master}
              }} = Policy.batch_read(base_retry, timeout: 250, socket_timeout: 100)
    end

    test "materializes encodable batch read fields" do
      filter = Exp.eq(Exp.int_bin("a"), Exp.val(1))

      assert {:ok,
              %Policy.BatchRead{
                filter: ^filter,
                read_mode_ap: :all,
                read_mode_sc: :linearize,
                read_touch_ttl_percent: 40
              }} =
               Policy.batch_read(RetryPolicy.defaults(),
                 filter: filter,
                 read_mode_ap: :all,
                 read_mode_sc: :linearize,
                 read_touch_ttl_percent: 40
               )
    end

    test "rejects unsupported batch opts" do
      assert {:error, %Error{code: :invalid_argument, message: message}} =
               Policy.batch_read(RetryPolicy.defaults(), max_retries: 1)

      assert message =~ "do not support option :max_retries"
    end
  end

  describe "batch/2" do
    test "materializes the shared batch runtime policy" do
      base_retry = %{max_retries: 5, sleep_between_retries_ms: 99, replica_policy: :sequence}

      assert {:ok,
              %Policy.Batch{
                timeout: 250,
                socket_timeout: 100,
                max_concurrent_nodes: 2,
                allow_partial_results: false,
                respond_all_keys: false,
                allow_inline: false,
                allow_inline_ssd: true,
                retry: %{max_retries: 0, sleep_between_retries_ms: 0, replica_policy: :sequence}
              }} =
               Policy.batch(base_retry,
                 timeout: 250,
                 socket_timeout: 100,
                 max_concurrent_nodes: 2,
                 allow_partial_results: false,
                 respond_all_keys: false,
                 allow_inline: false,
                 allow_inline_ssd: true
               )
    end

    test "rejects unsupported batch opts" do
      assert {:error, %Error{code: :invalid_argument, message: message}} =
               Policy.batch(RetryPolicy.defaults(), ttl: 10)

      assert message =~ "do not support option :ttl"
    end

    test "rejects invalid socket timeout values" do
      assert {:error, %Error{code: :invalid_argument, message: message}} =
               Policy.batch(RetryPolicy.defaults(), socket_timeout: -1)

      assert message =~ "socket_timeout must be a non-negative integer"
    end
  end

  describe "batch_record_write/1" do
    test "materializes encodable per-record write fields" do
      filter = Exp.eq(Exp.int_bin("a"), Exp.val(1))

      assert {:ok,
              %Policy.BatchWrite{
                ttl: 30,
                generation: 4,
                generation_policy: :expect_gt,
                filter: ^filter,
                exists: :replace_only,
                commit_level: :master,
                durable_delete: true,
                respond_per_op: true,
                send_key: true,
                read_mode_ap: :all,
                read_mode_sc: :allow_unavailable,
                read_touch_ttl_percent: 20
              }} =
               Policy.batch_record_write(
                 ttl: 30,
                 generation: 4,
                 generation_policy: :expect_gt,
                 filter: filter,
                 exists: :replace_only,
                 commit_level: :master,
                 durable_delete: true,
                 respond_per_op: true,
                 send_key: true,
                 read_mode_ap: :all,
                 read_mode_sc: :allow_unavailable,
                 read_touch_ttl_percent: 20
               )
    end

    test "rejects parent batch fields on per-record policies" do
      assert {:error, %Error{code: :invalid_argument, message: message}} =
               Policy.batch_record_write(timeout: 100)

      assert message =~ "do not support option :timeout"
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
      assert runtime.socket_timeout == 0
      assert runtime.task_timeout == :infinity
      assert runtime.pool_checkout_timeout == 5_000
      assert runtime.max_concurrent_nodes == 0
      assert runtime.retry == RetryPolicy.defaults()
      assert runtime.records_per_second == nil
      assert runtime.include_bin_data == true
      assert runtime.expected_duration == :long
      assert runtime.cursor == nil
      assert is_integer(runtime.task_id)
      assert runtime.task_id > 0
    end

    test "normalizes scan and query runtime policy fields" do
      base_retry = %{max_retries: 5, sleep_between_retries_ms: 10, replica_policy: :master}

      assert {:ok, %Policy.ScanQueryRuntime{} = runtime} =
               Policy.scan_query_runtime(base_retry,
                 timeout: 10_000,
                 socket_timeout: 250,
                 max_retries: 2,
                 sleep_between_retries_ms: 25,
                 replica_policy: :sequence,
                 max_concurrent_nodes: 3,
                 records_per_second: 40,
                 include_bin_data: false,
                 expected_duration: :short,
                 task_id: 123,
                 cursor: :resume,
                 pool_checkout_timeout: 75
               )

      assert runtime.timeout == 10_000
      assert runtime.socket_timeout == 250

      assert runtime.retry == %{
               max_retries: 2,
               sleep_between_retries_ms: 25,
               replica_policy: :sequence
             }

      assert runtime.max_concurrent_nodes == 3
      assert runtime.records_per_second == 40
      assert runtime.include_bin_data == false
      assert runtime.expected_duration == :short
      assert runtime.task_id == 123
      assert runtime.cursor == :resume
      assert runtime.pool_checkout_timeout == 75
    end

    test "rejects invalid task runtime values" do
      assert {:error, %Error{code: :invalid_argument, message: timeout_message}} =
               Policy.scan_query_runtime(task_timeout: :soon)

      assert timeout_message =~ ":task_timeout must be a non-negative integer or :infinity"

      assert {:error, %Error{code: :invalid_argument, message: task_id_message}} =
               Policy.scan_query_runtime(task_id: 0)

      assert task_id_message =~ ":task_id must be a positive integer"

      assert {:error, %Error{code: :invalid_argument, message: rps_message}} =
               Policy.scan_query_runtime(records_per_second: -1)

      assert rps_message =~ "records_per_second must be a non-negative integer"

      assert {:error, %Error{code: :invalid_argument, message: include_message}} =
               Policy.scan_query_runtime(include_bin_data: :maybe)

      assert include_message =~ "include_bin_data must be a boolean"

      assert {:error, %Error{code: :invalid_argument, message: duration_message}} =
               Policy.scan_query_runtime(expected_duration: :short_query)

      assert duration_message =~ "query duration must be one of"
    end
  end
end
