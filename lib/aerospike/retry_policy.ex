defmodule Aerospike.RetryPolicy do
  @moduledoc """
  Retry configuration and error classification for the command path.

  The retry driver in `Aerospike.Get` consumes a `t:t/0` value per command
  and decides, based on the classification helpers below, whether to
  re-dispatch an attempt against the next replica (on a rebalance-class
  error), re-dispatch against a fresh pool worker (on a transport-class
  error), or return the error verbatim (on anything else).

  ## Writer discipline

  The retry policy is cluster-scoped, not per-node, and is established
  once at `Aerospike.start_link/1` time. The Tender writes the effective
  policy to the `:meta` ETS table under the key `:retry_opts`; the
  command path reads it lock-free via `load/1`. Only the Tender writes
  this slot, matching the single-writer discipline that governs every
  other `:meta` entry.

  Per-command overrides (`:timeout`, `:max_retries`,
  `:sleep_between_retries_ms`, `:replica_policy`) may be passed through
  `Aerospike.get/3`'s `opts` and are merged on top of the cluster default
  by `merge/2`.

  ## Classification

  One canonical classifier drives the retry loop and the pool-side
  failure accounting. It returns:

    * `:bucket` — one of `:ok`, `:rebalance`, `:transport`,
      `:routing_refusal`, `:server_fatal`
    * `:retry_classification` — the retry telemetry label or `nil`
    * `:close_connection?` — whether the current worker should be
      discarded after the outcome
    * `:node_failure?` — whether the outcome should increment the
      node's `:failed` counter

  The buckets stay disjoint:

    * **rebalance** — the server replied with a result code that says
      "this partition is not mine right now" (currently
      `:partition_unavailable`). The retry driver re-picks on a
      different replica and asynchronously asks the Tender for a fresh
      partition map.

    * **transport** — the command did not reach a server that answered
      cleanly: `:network_error`, `:timeout`, `:connection_error`
      (socket), `:pool_timeout`, `:invalid_node` (pool checkout), and
      `:circuit_open` (Task 6 refusal). These are not ownership signals;
      the retry driver re-dispatches without asking for a map refresh.

    * **routing_refusal** — the router refused to select a replica
      (`:cluster_not_ready`, `:no_master`). The driver returns the atom
      verbatim; no retry.

    * **server_fatal** — everything else: server logical errors
      (`:key_not_found`, `:generation_error`, …) and client-local fatal
      errors like `:parse_error`. The driver returns these verbatim.
  """

  alias Aerospike.Error

  @default_max_retries 2
  @default_sleep_between_retries_ms 0
  @default_replica_policy :sequence

  # Transport-class error codes the retry driver treats as "try again
  # against a fresh pool worker or a different replica". Kept in sync
  # with `Aerospike.NodeCounters.@failure_codes` for the pool-side
  # classification plus two pool-level codes (`:pool_timeout`,
  # `:invalid_node`) the pool surfaces without touching the socket and
  # the breaker's `:circuit_open` refusal.
  @transport_codes [
    :network_error,
    :timeout,
    :connection_error,
    :pool_timeout,
    :invalid_node,
    :circuit_open
  ]
  @node_failure_codes [:network_error, :timeout, :connection_error]
  @routing_refusal_codes [:cluster_not_ready, :no_master]

  @meta_key :retry_opts

  @type replica_policy :: :master | :sequence
  @type bucket :: :ok | :rebalance | :transport | :routing_refusal | :server_fatal
  @type retry_classification :: :rebalance | :transport | :circuit_open | nil
  @type classification :: %{
          bucket: bucket(),
          retry_classification: retry_classification(),
          close_connection?: boolean(),
          node_failure?: boolean()
        }

  @typedoc """
  Effective retry policy for one command.

    * `:max_retries` — number of retries **after** the initial attempt
      (so a `:max_retries` of `2` means up to 3 attempts total). Must be
      a non-negative integer. `0` disables retry entirely.
    * `:sleep_between_retries_ms` — fixed delay between attempts; no
      jitter or exponential backoff.
    * `:replica_policy` — `:master` dispatches every attempt against the
      master replica (transport failures retry the same node); `:sequence`
      walks the replica list via `rem(attempt, length(replicas))` on each
      retry.
  """
  @type t :: %{
          max_retries: non_neg_integer(),
          sleep_between_retries_ms: non_neg_integer(),
          replica_policy: replica_policy()
        }

  @doc "Returns the default retry policy. Used by the Tender at init."
  @spec defaults() :: t()
  def defaults do
    %{
      max_retries: @default_max_retries,
      sleep_between_retries_ms: @default_sleep_between_retries_ms,
      replica_policy: @default_replica_policy
    }
  end

  @doc """
  Builds an effective retry policy by overlaying the keyword `opts` on
  top of `defaults/0`.

  Intended for the Tender's init path: validate the caller's start opts
  once and store the resulting map in `:meta`. Unknown keys are ignored
  so the retry policy can live alongside future policy knobs without a
  config migration.
  """
  @spec from_opts(keyword()) :: t()
  def from_opts(opts) when is_list(opts) do
    base = defaults()

    max_retries = fetch_non_neg_int(opts, :max_retries, base.max_retries)
    sleep_ms = fetch_non_neg_int(opts, :sleep_between_retries_ms, base.sleep_between_retries_ms)
    replica = fetch_replica_policy(opts, base.replica_policy)

    %{
      max_retries: max_retries,
      sleep_between_retries_ms: sleep_ms,
      replica_policy: replica
    }
  end

  @doc """
  Writes `policy` to `meta_tab` under the ETS key used by `load/1`.

  Only the Tender calls this. Single-writer invariant: no other process
  may update this slot.
  """
  @spec put(atom(), t()) :: true
  def put(
        meta_tab,
        %{
          max_retries: _,
          sleep_between_retries_ms: _,
          replica_policy: _
        } = policy
      )
      when is_atom(meta_tab) do
    :ets.insert(meta_tab, {@meta_key, policy})
  end

  @doc """
  Reads the cluster-default retry policy from the `:meta` ETS table.

  Falls back to `defaults/0` when the slot is absent so readers never
  crash against a Tender that was started without the retry plumbing
  (a cluster-state-only test harness, for example, that skips the
  retry-opts init).
  """
  @spec load(atom()) :: t()
  def load(meta_tab) when is_atom(meta_tab) do
    case :ets.lookup(meta_tab, @meta_key) do
      [{@meta_key, %{} = policy}] -> policy
      _ -> defaults()
    end
  end

  @doc """
  Overlays per-command `opts` on top of `base`. Only the three retry
  fields are recognised; other keys are ignored.
  """
  @spec merge(t(), keyword()) :: t()
  def merge(%{} = base, opts) when is_list(opts) do
    %{
      max_retries: fetch_non_neg_int(opts, :max_retries, base.max_retries),
      sleep_between_retries_ms:
        fetch_non_neg_int(opts, :sleep_between_retries_ms, base.sleep_between_retries_ms),
      replica_policy: fetch_replica_policy(opts, base.replica_policy)
    }
  end

  @doc """
  Classifies one command outcome into the Phase 1 retry buckets and the
  metadata the retry and pool layers consume.
  """
  @spec classify(term()) :: classification()
  def classify({:ok, _record}) do
    %{
      bucket: :ok,
      retry_classification: nil,
      close_connection?: false,
      node_failure?: false
    }
  end

  def classify({:error, %Error{} = err}), do: classify(err)

  def classify(%Error{} = err) do
    cond do
      Error.rebalance?(err) ->
        %{
          bucket: :rebalance,
          retry_classification: :rebalance,
          close_connection?: false,
          node_failure?: false
        }

      err.code in @transport_codes ->
        %{
          bucket: :transport,
          retry_classification: retry_label(err.code),
          close_connection?: err.code in @node_failure_codes,
          node_failure?: err.code in @node_failure_codes
        }

      err.code == :parse_error ->
        %{
          bucket: :server_fatal,
          retry_classification: nil,
          close_connection?: true,
          node_failure?: false
        }

      true ->
        %{
          bucket: :server_fatal,
          retry_classification: nil,
          close_connection?: false,
          node_failure?: false
        }
    end
  end

  def classify({:error, reason}) when reason in @routing_refusal_codes do
    %{
      bucket: :routing_refusal,
      retry_classification: nil,
      close_connection?: false,
      node_failure?: false
    }
  end

  def classify({:error, :unknown_node}) do
    %{
      bucket: :transport,
      retry_classification: :transport,
      close_connection?: false,
      node_failure?: false
    }
  end

  def classify(_other) do
    %{
      bucket: :server_fatal,
      retry_classification: nil,
      close_connection?: false,
      node_failure?: false
    }
  end

  @doc """
  Returns `true` when `term` is an error the retry driver should treat
  as a cluster-rebalance signal. Accepts either a bare `%Aerospike.Error{}`
  or the `{:error, _}` tuple form the command path produces; delegates to
  the canonical classifier above.
  """
  @spec rebalance?(term()) :: boolean()
  def rebalance?(term), do: classify(term).bucket == :rebalance

  @doc """
  Returns `true` when `term` is an error the retry driver should treat
  as a transport-class failure (re-dispatch without re-routing logic
  beyond the replica walk).

  Examples of transport-class codes: `:network_error`, `:timeout`,
  `:connection_error`, `:pool_timeout`, `:invalid_node`, `:circuit_open`.
  """
  @spec transport?(term()) :: boolean()
  def transport?(term), do: classify(term).bucket == :transport

  @doc """
  Returns the retry telemetry label for `term`, or `nil` when the
  outcome is fatal / non-retryable.
  """
  @spec retry_classification(term()) :: retry_classification()
  def retry_classification(term), do: classify(term).retry_classification

  @doc """
  Returns `true` when `term` should increment the node's `:failed`
  counter.
  """
  @spec node_failure?(term()) :: boolean()
  def node_failure?(term), do: classify(term).node_failure?

  ## Internals

  defp retry_label(:circuit_open), do: :circuit_open
  defp retry_label(_transport_code), do: :transport

  defp fetch_non_neg_int(opts, key, default) do
    case Keyword.fetch(opts, key) do
      {:ok, n} when is_integer(n) and n >= 0 ->
        n

      {:ok, other} ->
        raise ArgumentError,
              "Aerospike.RetryPolicy: #{inspect(key)} must be a non-negative integer, " <>
                "got #{inspect(other)}"

      :error ->
        default
    end
  end

  defp fetch_replica_policy(opts, default) do
    case Keyword.fetch(opts, :replica_policy) do
      {:ok, policy} when policy in [:master, :sequence] ->
        policy

      {:ok, other} ->
        raise ArgumentError,
              "Aerospike.RetryPolicy: :replica_policy must be :master or :sequence, " <>
                "got #{inspect(other)}"

      :error ->
        default
    end
  end
end
