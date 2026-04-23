defmodule Aerospike.Policy do
  @moduledoc """
  Canonical policy entry point for the spike's current command families.

  This module owns family-specific validation, defaults, and effective
  per-call policy materialization. Retry classification remains in the
  pure `Aerospike.RetryPolicy` sibling module; callers should ask
  `Aerospike.Policy` for effective policy data instead of merging raw
  keywords themselves.
  """

  alias Aerospike.Error
  alias Aerospike.RetryPolicy

  @default_timeout 5_000
  @default_pool_checkout_timeout 5_000

  defmodule ClusterDefaults do
    @moduledoc false

    @enforce_keys [:retry]
    defstruct [:retry]

    @type t :: %__MODULE__{retry: RetryPolicy.t()}
  end

  defmodule UnaryRead do
    @moduledoc false

    @enforce_keys [:timeout, :retry]
    defstruct [:timeout, :retry]

    @type t :: %__MODULE__{
            timeout: non_neg_integer(),
            retry: RetryPolicy.t()
          }
  end

  defmodule UnaryWrite do
    @moduledoc false

    @enforce_keys [:timeout, :retry, :ttl, :generation]
    defstruct [:timeout, :retry, :ttl, :generation]

    @type t :: %__MODULE__{
            timeout: non_neg_integer(),
            retry: RetryPolicy.t(),
            ttl: non_neg_integer(),
            generation: non_neg_integer()
          }
  end

  defmodule BatchRead do
    @moduledoc false

    @enforce_keys [:timeout, :retry]
    defstruct [:timeout, :retry]

    @type t :: %__MODULE__{
            timeout: non_neg_integer(),
            retry: RetryPolicy.t()
          }
  end

  defmodule Batch do
    @moduledoc false

    @enforce_keys [:timeout, :retry]
    defstruct [:timeout, :retry]

    @type t :: %__MODULE__{
            timeout: non_neg_integer(),
            retry: RetryPolicy.t()
          }
  end

  defmodule AdminInfo do
    @moduledoc false

    @enforce_keys [:pool_checkout_timeout]
    defstruct [:pool_checkout_timeout]

    @type t :: %__MODULE__{pool_checkout_timeout: non_neg_integer()}
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
      :task_timeout,
      :pool_checkout_timeout,
      :max_concurrent_nodes,
      :task_id,
      :cursor
    ]
    defstruct [
      :timeout,
      :task_timeout,
      :pool_checkout_timeout,
      :max_concurrent_nodes,
      :task_id,
      :cursor
    ]

    @type t :: %__MODULE__{
            timeout: non_neg_integer(),
            task_timeout: non_neg_integer() | :infinity,
            pool_checkout_timeout: non_neg_integer(),
            max_concurrent_nodes: non_neg_integer(),
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

  @spec unary_read(RetryPolicy.t(), keyword()) :: {:ok, UnaryRead.t()} | {:error, Error.t()}
  def unary_read(%{} = base_retry, opts) when is_list(opts) do
    with {:ok, timeout} <- fetch_non_neg_integer(opts, :timeout, @default_timeout) do
      {:ok, %UnaryRead{timeout: timeout, retry: RetryPolicy.merge(base_retry, opts)}}
    end
  end

  @spec unary_write(RetryPolicy.t(), keyword()) :: {:ok, UnaryWrite.t()} | {:error, Error.t()}
  def unary_write(%{} = base_retry, opts) when is_list(opts) do
    with {:ok, timeout} <- fetch_non_neg_integer(opts, :timeout, @default_timeout),
         {:ok, ttl} <- fetch_non_neg_integer(opts, :ttl, 0),
         {:ok, generation} <- fetch_non_neg_integer(opts, :generation, 0) do
      {:ok,
       %UnaryWrite{
         timeout: timeout,
         retry: RetryPolicy.merge(base_retry, opts),
         ttl: ttl,
         generation: generation
       }}
    end
  end

  @spec unary_operate(RetryPolicy.t(), keyword()) :: {:ok, UnaryWrite.t()} | {:error, Error.t()}
  def unary_operate(%{} = base_retry, opts) when is_list(opts) do
    unary_write(base_retry, opts)
  end

  @spec batch_read(RetryPolicy.t(), keyword()) :: {:ok, BatchRead.t()} | {:error, Error.t()}
  def batch_read(%{} = base_retry, opts) when is_list(opts) do
    with {:ok, %Batch{} = policy} <- batch(base_retry, opts) do
      {:ok, %BatchRead{timeout: policy.timeout, retry: policy.retry}}
    end
  end

  @spec batch(RetryPolicy.t(), keyword()) :: {:ok, Batch.t()} | {:error, Error.t()}
  def batch(%{} = base_retry, opts) when is_list(opts) do
    case Enum.find(opts, &invalid_batch_read_opt?/1) do
      nil ->
        {:ok,
         %Batch{
           timeout: Keyword.get(opts, :timeout, @default_timeout),
           retry: disable_retries(base_retry)
         }}

      {:timeout, _value} ->
        invalid_argument("Aerospike.batch_get/4 expects :timeout to be a non-negative integer")

      {key, _value} ->
        invalid_argument(
          "Aerospike.batch_get/4 currently supports only the :timeout option, got #{inspect(key)}"
        )
    end
  end

  @spec admin_info(keyword()) :: {:ok, AdminInfo.t()} | {:error, Error.t()}
  def admin_info(opts) when is_list(opts) do
    with {:ok, checkout_timeout} <-
           fetch_non_neg_integer(opts, :pool_checkout_timeout, @default_pool_checkout_timeout) do
      {:ok, %AdminInfo{pool_checkout_timeout: checkout_timeout}}
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
    with {:ok, timeout} <- fetch_non_neg_integer(opts, :timeout, @default_timeout),
         {:ok, task_timeout} <- fetch_task_timeout(opts),
         {:ok, checkout_timeout} <-
           fetch_non_neg_integer(opts, :pool_checkout_timeout, @default_pool_checkout_timeout),
         {:ok, max_concurrency} <- fetch_non_neg_integer(opts, :max_concurrent_nodes, 0),
         {:ok, task_id} <- fetch_task_id(opts) do
      {:ok,
       %ScanQueryRuntime{
         timeout: timeout,
         task_timeout: task_timeout,
         pool_checkout_timeout: checkout_timeout,
         max_concurrent_nodes: max_concurrency,
         task_id: task_id,
         cursor: Keyword.get(opts, :cursor)
       }}
    end
  end

  defp invalid_batch_read_opt?({:timeout, timeout}),
    do: not (is_integer(timeout) and timeout >= 0)

  defp invalid_batch_read_opt?({_key, _value}), do: true

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
    case Keyword.get(opts, :task_id, System.unique_integer([:positive])) do
      value when is_integer(value) and value > 0 ->
        {:ok, value}

      value ->
        invalid_argument(":task_id must be a positive integer, got: #{inspect(value)}")
    end
  end

  defp invalid_argument(message) do
    {:error, Error.from_result_code(:invalid_argument, message: message)}
  end
end
