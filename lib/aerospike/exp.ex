defmodule Aerospike.Exp do
  @moduledoc """
  Server-side filter expressions for policies (`:filter` on reads, batches, scans, queries).

  The full expression builder API (`Exp.gt/2`, `Exp.int_bin/1`, …) is planned; until then,
  `from_wire/1` wraps **already-encoded** filter bytes in the Aerospike wire format so
  advanced callers can attach filters from captured binaries or external encoders.

  ## Batch and `batch_get/3`

  Pass `filter: Exp.from_wire(bytes)` in batch options; the client attaches a `FILTER_EXP`
  field to the outer batch message.

  ## Related

  - `Aerospike.batch_get/3` — batch read options
  - `docs/design/api-proposal.md` Section 9 — expressions overview
  """

  @enforce_keys [:wire]
  defstruct [:wire]

  @type t :: %__MODULE__{wire: binary()}

  @doc """
  Wraps pre-encoded filter expression bytes for use in `:filter` options.

  The binary must match the server's filter expression wire layout.

  ## Examples

      iex> e = Aerospike.Exp.from_wire(<<1, 2>>)
      iex> e.wire
      <<1, 2>>

  """
  @spec from_wire(binary()) :: t()
  def from_wire(wire) when is_binary(wire), do: %__MODULE__{wire: wire}
end
