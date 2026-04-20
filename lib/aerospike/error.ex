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

  @impl true
  def message(%__MODULE__{code: code, message: msg}) do
    "Aerospike error #{code}: #{msg}"
  end
end
