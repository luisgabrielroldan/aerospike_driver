defmodule Aerospike.Policy do
  @moduledoc false

  alias Aerospike.Error
  alias Aerospike.Exp
  alias Aerospike.RetryPolicy

  @default_timeout 5_000
  @default_pool_checkout_timeout 5_000

  @expression_index_types [:numeric, :string, :geo2dsphere]
  @index_collection_types [:list, :mapkeys, :mapvalues]
  @expression_index_opts [:collection, :name, :pool_checkout_timeout, :type]
  @read_mode_ap_values [:one, :all]
  @read_mode_sc_values [:session, :linearize, :allow_replica, :allow_unavailable]
  @record_exists_actions [:update, :update_only, :create_or_replace, :replace_only, :create_only]
  @generation_policies [:none, :expect_equal, :expect_gt]
  @commit_levels [:all, :master]
  @query_durations [:long, :short, :long_relax_ap]
  @batch_parent_opts [
    :timeout,
    :socket_timeout,
    :max_concurrent_nodes,
    :allow_partial_results,
    :respond_all_keys,
    :allow_inline,
    :allow_inline_ssd
  ]
  @batch_read_opts [
                     :filter,
                     :read_mode_ap,
                     :read_mode_sc,
                     :read_touch_ttl_percent
                   ] ++ @batch_parent_opts
  @batch_record_read_opts [:filter, :read_mode_ap, :read_mode_sc, :read_touch_ttl_percent]
  @batch_record_write_opts [
    :ttl,
    :generation,
    :generation_policy,
    :filter,
    :exists,
    :commit_level,
    :durable_delete,
    :respond_per_op,
    :send_key,
    :read_mode_ap,
    :read_mode_sc,
    :read_touch_ttl_percent
  ]

  defmodule ClusterDefaults do
    @moduledoc false

    @enforce_keys [:retry]
    defstruct [:retry]

    @type t :: %__MODULE__{retry: RetryPolicy.t()}
  end

  defmodule UnaryRead do
    @moduledoc false

    @type read_mode_ap :: :one | :all
    @type read_mode_sc :: :session | :linearize | :allow_replica | :allow_unavailable

    @enforce_keys [
      :timeout,
      :socket_timeout,
      :retry,
      :filter,
      :read_mode_ap,
      :read_mode_sc,
      :read_touch_ttl_percent,
      :send_key,
      :use_compression
    ]
    defstruct [
      :timeout,
      :socket_timeout,
      :retry,
      :filter,
      :read_mode_ap,
      :read_mode_sc,
      :read_touch_ttl_percent,
      :send_key,
      :use_compression
    ]

    @type t :: %__MODULE__{
            timeout: non_neg_integer(),
            socket_timeout: non_neg_integer(),
            retry: RetryPolicy.t(),
            filter: Exp.t() | nil,
            read_mode_ap: read_mode_ap(),
            read_mode_sc: read_mode_sc(),
            read_touch_ttl_percent: -1 | 0..100,
            send_key: boolean(),
            use_compression: boolean() | nil
          }
  end

  defmodule UnaryWrite do
    @moduledoc false

    @type record_exists_action ::
            :update | :update_only | :create_or_replace | :replace_only | :create_only
    @type generation_policy :: :none | :expect_equal | :expect_gt
    @type commit_level :: :all | :master

    @enforce_keys [
      :timeout,
      :socket_timeout,
      :retry,
      :ttl,
      :generation,
      :generation_policy,
      :filter,
      :exists,
      :commit_level,
      :durable_delete,
      :respond_per_op,
      :send_key,
      :read_mode_ap,
      :read_mode_sc,
      :read_touch_ttl_percent,
      :use_compression
    ]
    defstruct [
      :timeout,
      :socket_timeout,
      :retry,
      :ttl,
      :generation,
      :generation_policy,
      :filter,
      :exists,
      :commit_level,
      :durable_delete,
      :respond_per_op,
      :send_key,
      :read_mode_ap,
      :read_mode_sc,
      :read_touch_ttl_percent,
      :use_compression
    ]

    @type t :: %__MODULE__{
            timeout: non_neg_integer(),
            socket_timeout: non_neg_integer(),
            retry: RetryPolicy.t(),
            ttl: integer(),
            generation: non_neg_integer(),
            generation_policy: generation_policy(),
            filter: Exp.t() | nil,
            exists: record_exists_action(),
            commit_level: commit_level(),
            durable_delete: boolean(),
            respond_per_op: boolean(),
            send_key: boolean(),
            read_mode_ap: UnaryRead.read_mode_ap(),
            read_mode_sc: UnaryRead.read_mode_sc(),
            read_touch_ttl_percent: -1 | 0..100,
            use_compression: boolean() | nil
          }
  end

  defmodule BatchRead do
    @moduledoc false

    @enforce_keys [
      :timeout,
      :socket_timeout,
      :retry,
      :max_concurrent_nodes,
      :allow_partial_results,
      :respond_all_keys,
      :allow_inline,
      :allow_inline_ssd,
      :filter,
      :read_mode_ap,
      :read_mode_sc,
      :read_touch_ttl_percent
    ]
    defstruct [
      :timeout,
      :socket_timeout,
      :retry,
      :max_concurrent_nodes,
      :allow_partial_results,
      :respond_all_keys,
      :allow_inline,
      :allow_inline_ssd,
      :filter,
      :read_mode_ap,
      :read_mode_sc,
      :read_touch_ttl_percent
    ]

    @type t :: %__MODULE__{
            timeout: non_neg_integer(),
            socket_timeout: non_neg_integer(),
            retry: RetryPolicy.t(),
            max_concurrent_nodes: non_neg_integer(),
            allow_partial_results: boolean(),
            respond_all_keys: boolean(),
            allow_inline: boolean(),
            allow_inline_ssd: boolean(),
            filter: Exp.t() | nil,
            read_mode_ap: UnaryRead.read_mode_ap(),
            read_mode_sc: UnaryRead.read_mode_sc(),
            read_touch_ttl_percent: -1 | 0..100
          }
  end

  defmodule Batch do
    @moduledoc false

    @enforce_keys [
      :timeout,
      :socket_timeout,
      :retry,
      :max_concurrent_nodes,
      :allow_partial_results,
      :respond_all_keys,
      :allow_inline,
      :allow_inline_ssd
    ]
    defstruct [
      :timeout,
      :socket_timeout,
      :retry,
      :max_concurrent_nodes,
      :allow_partial_results,
      :respond_all_keys,
      :allow_inline,
      :allow_inline_ssd
    ]

    @type t :: %__MODULE__{
            timeout: non_neg_integer(),
            socket_timeout: non_neg_integer(),
            retry: RetryPolicy.t(),
            max_concurrent_nodes: non_neg_integer(),
            allow_partial_results: boolean(),
            respond_all_keys: boolean(),
            allow_inline: boolean(),
            allow_inline_ssd: boolean()
          }
  end

  defmodule BatchWrite do
    @moduledoc false

    @enforce_keys [
      :ttl,
      :generation,
      :generation_policy,
      :filter,
      :exists,
      :commit_level,
      :durable_delete,
      :respond_per_op,
      :send_key,
      :read_mode_ap,
      :read_mode_sc,
      :read_touch_ttl_percent
    ]
    defstruct [
      :ttl,
      :generation,
      :generation_policy,
      :filter,
      :exists,
      :commit_level,
      :durable_delete,
      :respond_per_op,
      :send_key,
      :read_mode_ap,
      :read_mode_sc,
      :read_touch_ttl_percent
    ]

    @type t :: %__MODULE__{
            ttl: integer(),
            generation: non_neg_integer(),
            generation_policy: UnaryWrite.generation_policy(),
            filter: Exp.t() | nil,
            exists: UnaryWrite.record_exists_action(),
            commit_level: UnaryWrite.commit_level(),
            durable_delete: boolean(),
            respond_per_op: boolean(),
            send_key: boolean(),
            read_mode_ap: UnaryRead.read_mode_ap(),
            read_mode_sc: UnaryRead.read_mode_sc(),
            read_touch_ttl_percent: -1 | 0..100
          }
  end

  defmodule AdminInfo do
    @moduledoc false

    @enforce_keys [:pool_checkout_timeout]
    defstruct [:pool_checkout_timeout]

    @type t :: %__MODULE__{pool_checkout_timeout: non_neg_integer()}
  end

  defmodule ExpressionIndexCreate do
    @moduledoc false

    @enforce_keys [:name, :type, :collection, :pool_checkout_timeout]
    defstruct [:name, :type, :collection, :pool_checkout_timeout]

    @type t :: %__MODULE__{
            name: String.t(),
            type: :numeric | :string | :geo2dsphere,
            collection: :list | :mapkeys | :mapvalues | nil,
            pool_checkout_timeout: non_neg_integer()
          }
  end

  defmodule SecurityAdmin do
    @moduledoc false

    @enforce_keys [:timeout, :pool_checkout_timeout]
    defstruct [:timeout, :pool_checkout_timeout]

    @type t :: %__MODULE__{
            timeout: non_neg_integer(),
            pool_checkout_timeout: non_neg_integer()
          }
  end

  defmodule ScanQueryRuntime do
    @moduledoc false

    @enforce_keys [
      :timeout,
      :socket_timeout,
      :task_timeout,
      :pool_checkout_timeout,
      :max_concurrent_nodes,
      :retry,
      :records_per_second,
      :include_bin_data,
      :expected_duration,
      :task_id,
      :cursor
    ]
    defstruct [
      :timeout,
      :socket_timeout,
      :task_timeout,
      :pool_checkout_timeout,
      :max_concurrent_nodes,
      :retry,
      :records_per_second,
      :include_bin_data,
      :expected_duration,
      :task_id,
      :cursor
    ]

    @type t :: %__MODULE__{
            timeout: non_neg_integer(),
            socket_timeout: non_neg_integer(),
            task_timeout: non_neg_integer() | :infinity,
            pool_checkout_timeout: non_neg_integer(),
            max_concurrent_nodes: non_neg_integer(),
            retry: RetryPolicy.t(),
            records_per_second: non_neg_integer() | nil,
            include_bin_data: boolean(),
            expected_duration: :long | :short | :long_relax_ap,
            task_id: pos_integer(),
            cursor: term()
          }
  end

  @spec cluster_defaults(keyword()) :: ClusterDefaults.t()
  def cluster_defaults(opts) when is_list(opts) do
    %ClusterDefaults{retry: RetryPolicy.from_opts(opts)}
  end

  @spec admin_checkout_timeout(AdminInfo.t()) :: non_neg_integer()
  def admin_checkout_timeout(%AdminInfo{pool_checkout_timeout: timeout}), do: timeout

  @spec security_admin_timeout(SecurityAdmin.t()) :: non_neg_integer()
  def security_admin_timeout(%SecurityAdmin{timeout: timeout}), do: timeout

  @spec security_admin_checkout_timeout(SecurityAdmin.t()) :: non_neg_integer()
  def security_admin_checkout_timeout(%SecurityAdmin{pool_checkout_timeout: timeout}),
    do: timeout

  @spec batch_read(keyword()) :: {:ok, BatchRead.t()} | {:error, Error.t()}
  def batch_read(opts) when is_list(opts) do
    batch_read(RetryPolicy.defaults(), opts)
  end

  @spec batch(keyword()) :: {:ok, Batch.t()} | {:error, Error.t()}
  def batch(opts) when is_list(opts) do
    batch(RetryPolicy.defaults(), opts)
  end

  @spec batch_parent_opts(keyword()) :: keyword()
  def batch_parent_opts(opts) when is_list(opts) do
    Keyword.take(opts, @batch_parent_opts)
  end

  @spec batch_record_opts(keyword()) :: keyword()
  def batch_record_opts(opts) when is_list(opts) do
    Keyword.drop(opts, @batch_parent_opts)
  end

  @spec unary_read(RetryPolicy.t(), keyword()) :: {:ok, UnaryRead.t()} | {:error, Error.t()}
  def unary_read(%{} = base_retry, opts) when is_list(opts) do
    with {:ok, timeout} <- fetch_non_neg_integer(opts, :timeout, @default_timeout),
         {:ok, socket_timeout} <- fetch_non_neg_integer(opts, :socket_timeout, 0),
         {:ok, filter} <- fetch_filter(opts),
         {:ok, read_mode_ap} <-
           fetch_optional_member(opts, :read_mode_ap, @read_mode_ap_values, "AP read mode"),
         {:ok, read_mode_sc} <-
           fetch_optional_member(opts, :read_mode_sc, @read_mode_sc_values, "SC read mode"),
         {:ok, read_touch_ttl_percent} <- fetch_read_touch_ttl_percent(opts),
         {:ok, send_key} <- fetch_boolean(opts, :send_key, false),
         {:ok, use_compression} <- fetch_optional_boolean(opts, :use_compression) do
      {:ok,
       %UnaryRead{
         timeout: timeout,
         socket_timeout: socket_timeout,
         retry: RetryPolicy.merge(base_retry, opts),
         filter: filter,
         read_mode_ap: read_mode_ap || :one,
         read_mode_sc: read_mode_sc || :session,
         read_touch_ttl_percent: read_touch_ttl_percent,
         send_key: send_key,
         use_compression: use_compression
       }}
    end
  end

  @spec unary_write(RetryPolicy.t(), keyword()) :: {:ok, UnaryWrite.t()} | {:error, Error.t()}
  def unary_write(%{} = base_retry, opts) when is_list(opts) do
    with {:ok, timeout} <- fetch_non_neg_integer(opts, :timeout, @default_timeout),
         {:ok, socket_timeout} <- fetch_non_neg_integer(opts, :socket_timeout, 0),
         {:ok, ttl} <- fetch_ttl(opts),
         {:ok, generation} <- fetch_non_neg_integer(opts, :generation, 0),
         {:ok, generation_policy} <- fetch_generation_policy(opts, generation),
         {:ok, filter} <- fetch_filter(opts),
         {:ok, exists} <- fetch_record_exists_action(opts),
         {:ok, commit_level} <-
           fetch_optional_member(opts, :commit_level, @commit_levels, "commit level"),
         {:ok, durable_delete} <- fetch_boolean(opts, :durable_delete, false),
         {:ok, respond_per_op} <- fetch_boolean(opts, :respond_per_op, false),
         {:ok, send_key} <- fetch_boolean(opts, :send_key, true),
         {:ok, read_mode_ap} <-
           fetch_optional_member(opts, :read_mode_ap, @read_mode_ap_values, "AP read mode"),
         {:ok, read_mode_sc} <-
           fetch_optional_member(opts, :read_mode_sc, @read_mode_sc_values, "SC read mode"),
         {:ok, read_touch_ttl_percent} <- fetch_read_touch_ttl_percent(opts),
         {:ok, use_compression} <- fetch_optional_boolean(opts, :use_compression) do
      {:ok,
       %UnaryWrite{
         timeout: timeout,
         socket_timeout: socket_timeout,
         retry: RetryPolicy.merge(base_retry, opts),
         ttl: ttl,
         generation: generation,
         generation_policy: generation_policy,
         filter: filter,
         exists: exists,
         commit_level: commit_level || :all,
         durable_delete: durable_delete,
         respond_per_op: respond_per_op,
         send_key: send_key,
         read_mode_ap: read_mode_ap || :one,
         read_mode_sc: read_mode_sc || :session,
         read_touch_ttl_percent: read_touch_ttl_percent,
         use_compression: use_compression
       }}
    end
  end

  @spec unary_operate(RetryPolicy.t(), keyword()) :: {:ok, UnaryWrite.t()} | {:error, Error.t()}
  def unary_operate(%{} = base_retry, opts) when is_list(opts) do
    unary_write(base_retry, opts)
  end

  @spec batch_read(RetryPolicy.t(), keyword()) :: {:ok, BatchRead.t()} | {:error, Error.t()}
  def batch_read(%{} = base_retry, opts) when is_list(opts) do
    with :ok <- reject_unknown_opts(opts, @batch_read_opts, "Aerospike batch read helpers"),
         {:ok, %Batch{} = policy} <- batch(base_retry, batch_parent_opts(opts)),
         {:ok, filter} <- fetch_filter(opts),
         {:ok, read_mode_ap} <-
           fetch_optional_member(opts, :read_mode_ap, @read_mode_ap_values, "AP read mode"),
         {:ok, read_mode_sc} <-
           fetch_optional_member(opts, :read_mode_sc, @read_mode_sc_values, "SC read mode"),
         {:ok, read_touch_ttl_percent} <- fetch_read_touch_ttl_percent(opts) do
      {:ok,
       %BatchRead{
         timeout: policy.timeout,
         socket_timeout: policy.socket_timeout,
         retry: policy.retry,
         max_concurrent_nodes: policy.max_concurrent_nodes,
         allow_partial_results: policy.allow_partial_results,
         respond_all_keys: policy.respond_all_keys,
         allow_inline: policy.allow_inline,
         allow_inline_ssd: policy.allow_inline_ssd,
         filter: filter,
         read_mode_ap: read_mode_ap || :one,
         read_mode_sc: read_mode_sc || :session,
         read_touch_ttl_percent: read_touch_ttl_percent
       }}
    end
  end

  @spec batch(RetryPolicy.t(), keyword()) :: {:ok, Batch.t()} | {:error, Error.t()}
  def batch(%{} = base_retry, opts) when is_list(opts) do
    with :ok <- reject_unknown_opts(opts, @batch_parent_opts, "Aerospike batch helpers"),
         {:ok, timeout} <- fetch_non_neg_integer(opts, :timeout, @default_timeout),
         {:ok, socket_timeout} <- fetch_non_neg_integer(opts, :socket_timeout, 0),
         {:ok, max_concurrent_nodes} <- fetch_non_neg_integer(opts, :max_concurrent_nodes, 0),
         {:ok, allow_partial_results} <- fetch_boolean(opts, :allow_partial_results, true),
         {:ok, respond_all_keys} <- fetch_boolean(opts, :respond_all_keys, true),
         {:ok, allow_inline} <- fetch_boolean(opts, :allow_inline, true),
         {:ok, allow_inline_ssd} <- fetch_boolean(opts, :allow_inline_ssd, false) do
      {:ok,
       %Batch{
         timeout: timeout,
         socket_timeout: socket_timeout,
         retry: disable_retries(base_retry),
         max_concurrent_nodes: max_concurrent_nodes,
         allow_partial_results: allow_partial_results,
         respond_all_keys: respond_all_keys,
         allow_inline: allow_inline,
         allow_inline_ssd: allow_inline_ssd
       }}
    end
  end

  @spec batch_record_read(keyword()) :: {:ok, BatchRead.t()} | {:error, Error.t()}
  def batch_record_read(opts) when is_list(opts) do
    with :ok <-
           reject_unknown_opts(opts, @batch_record_read_opts, "Aerospike batch read records"),
         {:ok, filter} <- fetch_filter(opts),
         {:ok, read_mode_ap} <-
           fetch_optional_member(opts, :read_mode_ap, @read_mode_ap_values, "AP read mode"),
         {:ok, read_mode_sc} <-
           fetch_optional_member(opts, :read_mode_sc, @read_mode_sc_values, "SC read mode"),
         {:ok, read_touch_ttl_percent} <- fetch_read_touch_ttl_percent(opts) do
      {:ok,
       %BatchRead{
         timeout: @default_timeout,
         socket_timeout: 0,
         retry: disable_retries(RetryPolicy.defaults()),
         max_concurrent_nodes: 0,
         allow_partial_results: true,
         respond_all_keys: true,
         allow_inline: true,
         allow_inline_ssd: false,
         filter: filter,
         read_mode_ap: read_mode_ap || :one,
         read_mode_sc: read_mode_sc || :session,
         read_touch_ttl_percent: read_touch_ttl_percent
       }}
    end
  end

  @spec batch_record_write(keyword()) :: {:ok, BatchWrite.t()} | {:error, Error.t()}
  def batch_record_write(opts) when is_list(opts) do
    with :ok <-
           reject_unknown_opts(opts, @batch_record_write_opts, "Aerospike batch write records"),
         {:ok, ttl} <- fetch_ttl(opts),
         {:ok, generation} <- fetch_non_neg_integer(opts, :generation, 0),
         {:ok, generation_policy} <- fetch_generation_policy(opts, generation),
         {:ok, filter} <- fetch_filter(opts),
         {:ok, exists} <- fetch_record_exists_action(opts),
         {:ok, commit_level} <-
           fetch_optional_member(opts, :commit_level, @commit_levels, "commit level"),
         {:ok, durable_delete} <- fetch_boolean(opts, :durable_delete, false),
         {:ok, respond_per_op} <- fetch_boolean(opts, :respond_per_op, false),
         {:ok, send_key} <- fetch_boolean(opts, :send_key, false),
         {:ok, read_mode_ap} <-
           fetch_optional_member(opts, :read_mode_ap, @read_mode_ap_values, "AP read mode"),
         {:ok, read_mode_sc} <-
           fetch_optional_member(opts, :read_mode_sc, @read_mode_sc_values, "SC read mode"),
         {:ok, read_touch_ttl_percent} <- fetch_read_touch_ttl_percent(opts) do
      {:ok,
       %BatchWrite{
         ttl: ttl,
         generation: generation,
         generation_policy: generation_policy,
         filter: filter,
         exists: exists,
         commit_level: commit_level || :all,
         durable_delete: durable_delete,
         respond_per_op: respond_per_op,
         send_key: send_key,
         read_mode_ap: read_mode_ap || :one,
         read_mode_sc: read_mode_sc || :session,
         read_touch_ttl_percent: read_touch_ttl_percent
       }}
    end
  end

  @spec admin_info(keyword()) :: {:ok, AdminInfo.t()} | {:error, Error.t()}
  def admin_info(opts) when is_list(opts) do
    with {:ok, checkout_timeout} <-
           fetch_non_neg_integer(opts, :pool_checkout_timeout, @default_pool_checkout_timeout) do
      {:ok, %AdminInfo{pool_checkout_timeout: checkout_timeout}}
    end
  end

  @spec expression_index_create(Exp.t(), keyword()) ::
          {:ok, ExpressionIndexCreate.t()} | {:error, Error.t()}
  def expression_index_create(%Exp{wire: wire}, opts)
      when is_binary(wire) and byte_size(wire) > 0 and is_list(opts) do
    with :ok <- reject_unknown_expression_index_opts(opts),
         {:ok, name} <- fetch_required_string(opts, :name, "expression-backed index name"),
         {:ok, type} <-
           fetch_required_member(
             opts,
             :type,
             @expression_index_types,
             "expression-backed index type"
           ),
         {:ok, collection} <-
           fetch_optional_member(
             opts,
             :collection,
             @index_collection_types,
             "expression-backed index collection type"
           ),
         {:ok, checkout_timeout} <-
           fetch_non_neg_integer(opts, :pool_checkout_timeout, @default_pool_checkout_timeout) do
      {:ok,
       %ExpressionIndexCreate{
         name: name,
         type: type,
         collection: collection,
         pool_checkout_timeout: checkout_timeout
       }}
    end
  end

  def expression_index_create(%Exp{}, opts) when is_list(opts) do
    invalid_argument(
      "expression-backed indexes require a %Aerospike.Exp{} with non-empty wire bytes"
    )
  end

  @spec xdr_filter(String.t(), String.t(), Exp.t() | nil) :: :ok | {:error, Error.t()}
  def xdr_filter(datacenter, namespace, filter)
      when is_binary(datacenter) and is_binary(namespace) do
    with :ok <- validate_info_identifier(:datacenter, datacenter),
         :ok <- validate_info_identifier(:namespace, namespace) do
      validate_xdr_filter_expression(filter)
    end
  end

  @spec security_admin(keyword()) :: {:ok, SecurityAdmin.t()} | {:error, Error.t()}
  def security_admin(opts) when is_list(opts) do
    case Enum.find(opts, &invalid_security_admin_opt?/1) do
      nil ->
        with {:ok, timeout} <- fetch_non_neg_integer(opts, :timeout, @default_timeout),
             {:ok, checkout_timeout} <-
               fetch_non_neg_integer(
                 opts,
                 :pool_checkout_timeout,
                 @default_pool_checkout_timeout
               ) do
          {:ok, %SecurityAdmin{timeout: timeout, pool_checkout_timeout: checkout_timeout}}
        end

      {:timeout, _value} ->
        invalid_argument("security admin commands expect :timeout to be a non-negative integer")

      {:pool_checkout_timeout, _value} ->
        invalid_argument(
          "security admin commands expect :pool_checkout_timeout to be a non-negative integer"
        )

      {key, _value} ->
        invalid_argument(
          "security admin commands support only :timeout and :pool_checkout_timeout options, got #{inspect(key)}"
        )
    end
  end

  @spec scan_query_runtime(keyword()) :: {:ok, ScanQueryRuntime.t()} | {:error, Error.t()}
  def scan_query_runtime(opts) when is_list(opts) do
    scan_query_runtime(RetryPolicy.defaults(), opts)
  end

  @spec scan_query_runtime(RetryPolicy.t(), keyword()) ::
          {:ok, ScanQueryRuntime.t()} | {:error, Error.t()}
  def scan_query_runtime(%{} = base_retry, opts) when is_list(opts) do
    with {:ok, timeout} <- fetch_non_neg_integer(opts, :timeout, @default_timeout),
         {:ok, socket_timeout} <- fetch_non_neg_integer(opts, :socket_timeout, 0),
         {:ok, task_timeout} <- fetch_task_timeout(opts),
         {:ok, checkout_timeout} <-
           fetch_non_neg_integer(opts, :pool_checkout_timeout, @default_pool_checkout_timeout),
         {:ok, max_concurrency} <- fetch_non_neg_integer(opts, :max_concurrent_nodes, 0),
         {:ok, records_per_second} <- fetch_optional_non_neg_integer(opts, :records_per_second),
         {:ok, include_bin_data} <- fetch_boolean(opts, :include_bin_data, true),
         {:ok, expected_duration} <-
           fetch_optional_member(opts, :expected_duration, @query_durations, "query duration"),
         {:ok, task_id} <- fetch_task_id(opts) do
      {:ok,
       %ScanQueryRuntime{
         timeout: timeout,
         socket_timeout: socket_timeout,
         task_timeout: task_timeout,
         pool_checkout_timeout: checkout_timeout,
         max_concurrent_nodes: max_concurrency,
         retry: RetryPolicy.merge(base_retry, opts),
         records_per_second: records_per_second,
         include_bin_data: include_bin_data,
         expected_duration: expected_duration || :long,
         task_id: task_id,
         cursor: Keyword.get(opts, :cursor)
       }}
    end
  end

  defp invalid_security_admin_opt?({:timeout, timeout}),
    do: not (is_integer(timeout) and timeout >= 0)

  defp invalid_security_admin_opt?({:pool_checkout_timeout, timeout}),
    do: not (is_integer(timeout) and timeout >= 0)

  defp invalid_security_admin_opt?({_key, _value}), do: true

  defp disable_retries(base_retry) do
    %{base_retry | max_retries: 0, sleep_between_retries_ms: 0}
  end

  defp fetch_non_neg_integer(opts, key, default) do
    case Keyword.get(opts, key, default) do
      value when is_integer(value) and value >= 0 ->
        {:ok, value}

      value ->
        invalid_argument("#{key} must be a non-negative integer, got: #{inspect(value)}")
    end
  end

  defp fetch_optional_non_neg_integer(opts, key) do
    case Keyword.fetch(opts, key) do
      :error ->
        {:ok, nil}

      {:ok, value} when is_integer(value) and value >= 0 ->
        {:ok, value}

      {:ok, value} ->
        invalid_argument("#{key} must be a non-negative integer, got: #{inspect(value)}")
    end
  end

  defp fetch_boolean(opts, key, default) do
    case Keyword.get(opts, key, default) do
      value when is_boolean(value) ->
        {:ok, value}

      value ->
        invalid_argument("#{key} must be a boolean, got: #{inspect(value)}")
    end
  end

  defp fetch_optional_boolean(opts, key) do
    case Keyword.fetch(opts, key) do
      :error -> {:ok, nil}
      {:ok, value} when is_boolean(value) -> {:ok, value}
      {:ok, value} -> invalid_argument("#{key} must be a boolean, got: #{inspect(value)}")
    end
  end

  defp reject_unknown_opts(opts, allowed, context) do
    allowed = MapSet.new(allowed)

    case Enum.find(opts, fn {key, _value} -> not MapSet.member?(allowed, key) end) do
      nil -> :ok
      {key, _value} -> invalid_argument("#{context} do not support option #{inspect(key)}")
    end
  end

  defp fetch_ttl(opts) do
    case Keyword.get(opts, :ttl, 0) do
      :default ->
        {:ok, 0}

      :never_expire ->
        {:ok, -1}

      :dont_update ->
        {:ok, -2}

      value when is_integer(value) and value >= 0 ->
        {:ok, value}

      value ->
        invalid_argument(
          ":ttl must be a non-negative integer, :default, :never_expire, or :dont_update, got: #{inspect(value)}"
        )
    end
  end

  defp fetch_generation_policy(opts, generation) do
    case fetch_optional_member(
           opts,
           :generation_policy,
           @generation_policies,
           "generation write policy"
         ) do
      {:ok, nil} when generation > 0 -> {:ok, :expect_equal}
      {:ok, nil} -> {:ok, :none}
      other -> other
    end
  end

  defp fetch_read_touch_ttl_percent(opts) do
    case Keyword.get(opts, :read_touch_ttl_percent, 0) do
      -1 ->
        {:ok, -1}

      value when is_integer(value) and value in 0..100 ->
        {:ok, value}

      value ->
        invalid_argument(
          ":read_touch_ttl_percent must be -1, 0, or an integer from 1 through 100, got: #{inspect(value)}"
        )
    end
  end

  defp fetch_task_timeout(opts) do
    case Keyword.get(opts, :task_timeout, :infinity) do
      :infinity ->
        {:ok, :infinity}

      value when is_integer(value) and value >= 0 ->
        {:ok, value}

      value ->
        invalid_argument(
          ":task_timeout must be a non-negative integer or :infinity, got: #{inspect(value)}"
        )
    end
  end

  defp fetch_task_id(opts) do
    case Keyword.get(opts, :task_id, random_task_id()) do
      value when is_integer(value) and value > 0 ->
        {:ok, value}

      value ->
        invalid_argument(":task_id must be a positive integer, got: #{inspect(value)}")
    end
  end

  defp fetch_required_string(opts, key, label) do
    case Keyword.fetch(opts, key) do
      {:ok, value} when is_binary(value) and value != "" ->
        {:ok, value}

      {:ok, value} ->
        invalid_argument("#{label} must be a non-empty string, got: #{inspect(value)}")

      :error ->
        invalid_argument("expression-backed indexes require option :#{key}")
    end
  end

  defp fetch_required_member(opts, key, allowed, label) do
    case Keyword.fetch(opts, key) do
      {:ok, value} ->
        if value in allowed do
          {:ok, value}
        else
          invalid_argument("#{label} must be one of #{inspect(allowed)}, got: #{inspect(value)}")
        end

      :error ->
        invalid_argument("expression-backed indexes require option :#{key}")
    end
  end

  defp fetch_optional_member(opts, key, allowed, label) do
    case Keyword.get(opts, key) do
      nil ->
        {:ok, nil}

      value ->
        if value in allowed do
          {:ok, value}
        else
          invalid_argument("#{label} must be one of #{inspect(allowed)}, got: #{inspect(value)}")
        end
    end
  end

  defp fetch_filter(opts) do
    case Keyword.get(opts, :filter) do
      nil ->
        {:ok, nil}

      %Exp{wire: wire} = filter when is_binary(wire) and byte_size(wire) > 0 ->
        {:ok, filter}

      value ->
        invalid_argument(
          ":filter must be nil or a %Aerospike.Exp{} with non-empty wire bytes, got: #{inspect(value)}"
        )
    end
  end

  defp fetch_record_exists_action(opts) do
    case fetch_optional_member(
           opts,
           :exists,
           @record_exists_actions,
           "record-exists write policy"
         ) do
      {:ok, nil} -> {:ok, :update}
      other -> other
    end
  end

  defp validate_info_identifier(label, value) when value == "" do
    invalid_argument("#{label} must be a non-empty string")
  end

  defp validate_info_identifier(label, value) when is_binary(value) do
    if String.contains?(value, [";", "=", "\n", "\r", "\t"]) do
      invalid_argument("#{label} cannot contain info-command delimiters")
    else
      :ok
    end
  end

  defp validate_xdr_filter_expression(nil), do: :ok

  defp validate_xdr_filter_expression(%Exp{wire: wire})
       when is_binary(wire) and byte_size(wire) > 0 do
    :ok
  end

  defp validate_xdr_filter_expression(value) do
    invalid_argument(
      "XDR filters require nil or a %Aerospike.Exp{} with non-empty wire bytes, got: #{inspect(value)}"
    )
  end

  defp reject_unknown_expression_index_opts(opts) do
    case Enum.find(opts, fn {key, _value} -> key not in @expression_index_opts end) do
      nil ->
        :ok

      {key, _value} ->
        invalid_argument(
          "expression-backed indexes support only :name, :type, :collection, and :pool_checkout_timeout options, got #{inspect(key)}"
        )
    end
  end

  # Aerospike foreground/background scan and query task ids are uint64 values.
  # Official clients generate random 64-bit ids because reusing a small
  # per-process/per-VM counter can be rejected by the server across rounds.
  defp random_task_id do
    case :crypto.strong_rand_bytes(8) do
      <<0::64>> -> random_task_id()
      <<task_id::64-unsigned-big>> -> task_id
    end
  end

  defp invalid_argument(message) do
    {:error, Error.from_result_code(:invalid_argument, message: message)}
  end
end
