defmodule Aerospike.Error do
  @moduledoc """
  Exception raised for Aerospike client and server errors.

  Result codes from the wire protocol are represented as atoms (e.g. `:key_not_found`,
  `:timeout`). Use `from_result_code/2` to build an error from a result code atom.
  """

  alias Aerospike.Protocol.ResultCode

  @enforce_keys [:code, :message]
  defexception [:code, :message, node: nil, in_doubt: false]

  @type t :: %__MODULE__{
          code: atom(),
          message: String.t(),
          node: String.t() | nil,
          in_doubt: boolean()
        }

  @doc """
  Builds an error struct from a result code atom.

  The default human-readable message is derived from the result code.
  Options:

  * `:message` — override the message string
  * `:node` — optional node name (for cluster-aware errors)
  * `:in_doubt` — whether the outcome is uncertain (default `false`)

  ## Examples

      iex> e = Aerospike.Error.from_result_code(:key_not_found)
      iex> e.code
      :key_not_found
      iex> e.in_doubt
      false

      iex> e = Aerospike.Error.from_result_code(:timeout, node: "BB9", in_doubt: true)
      iex> e.node
      "BB9"
      iex> e.in_doubt
      true

  """
  @spec from_result_code(atom(), keyword()) :: t()
  def from_result_code(code, opts \\ []) when is_atom(code) do
    default_msg = ResultCode.message(code)

    %__MODULE__{
      code: code,
      message: Keyword.get(opts, :message, default_msg),
      node: Keyword.get(opts, :node, nil),
      in_doubt: Keyword.get(opts, :in_doubt, false)
    }
  end

  # Server result codes that indicate the partition map the client addressed
  # is out of sync with the cluster's current ownership. These are a routing
  # signal — retry against a different replica (and trigger a tend) — not a
  # node-health signal.
  #
  # Today only `:partition_unavailable` (wire code 11) qualifies. The Go
  # and Java reference clients branch on this exact code: Go's
  # `multi_command.go` treats `PARTITION_UNAVAILABLE` as a re-route cue
  # distinct from generic server errors, and Java's `PartitionTracker`
  # flips a partition's `retry` flag on the same code. If the Aerospike
  # server grows another "not-mine" result code in the future, widen this
  # list after auditing the reference clients.
  @rebalance_codes [:partition_unavailable]

  @doc """
  Returns `true` when the error represents a cluster rebalance / partition-
  ownership signal that should trigger a re-route rather than a same-replica
  retry.

  Rebalance-class errors mean "the server you addressed does not own this
  partition right now" — the retry layer consumes this as a cue to pick the
  next replica (and usually to ask the Tender for a fresh partition map).
  Transport errors (`:network_error`, `:timeout`, pool errors) and server
  errors unrelated to ownership (`:key_not_found`, `:generation_error`,
  etc.) are **not** rebalance-class.

  Accepts `t/0` or any value; non-`Error` values (including bare atoms like
  `:cluster_not_ready`) return `false` so callers can pattern-match uniformly
  on whatever the command path returned.

  ## Examples

      iex> err = Aerospike.Error.from_result_code(:partition_unavailable)
      iex> Aerospike.Error.rebalance?(err)
      true

      iex> err = Aerospike.Error.from_result_code(:key_not_found)
      iex> Aerospike.Error.rebalance?(err)
      false

      iex> Aerospike.Error.rebalance?(:cluster_not_ready)
      false

  """
  @spec rebalance?(t() | term()) :: boolean()
  def rebalance?(%__MODULE__{code: code}), do: code in @rebalance_codes
  def rebalance?(_other), do: false

  @impl true
  def message(%__MODULE__{code: code, message: msg}) do
    "Aerospike error #{code}: #{msg}"
  end
end
