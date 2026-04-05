defmodule Aerospike.Protocol.OperateFlags do
  @moduledoc false

  # Scans an operation list to derive AsmMsg info1/info2 flags for operate commands.
  # Shared by single-record `CRUD.encode_operate/3` and batch operate encoding.

  import Bitwise

  alias Aerospike.Protocol.AsmMsg
  alias Aerospike.Protocol.AsmMsg.Operation

  @typedoc """
  Result of scanning an operation list for wire-level flags.

  - `info1`, `info2`, `info3` — accumulated flag bytes for the AsmMsg header
  - `has_write?` — true if any operation modifies data (write, add, append, CDT modify, etc.)
  - `read_bin?` — true if any operation reads bin data
  - `read_header?` — true if any operation reads only record metadata (generation/TTL)
  - `respond_all?` — true if RESPOND_ALL_OPS flag should be set (certain CDT/HLL/bit ops)
  - `header_only?` — derived: read_header? and not read_bin?
  """
  @type t :: %{
          info1: non_neg_integer(),
          info2: non_neg_integer(),
          info3: non_neg_integer(),
          has_write?: boolean(),
          read_bin?: boolean(),
          read_header?: boolean(),
          respond_all?: boolean(),
          header_only?: boolean()
        }

  @read Operation.op_read()
  @write Operation.op_write()
  @cdt_read Operation.op_cdt_read()
  @cdt_modify Operation.op_cdt_modify()
  @add Operation.op_add()
  @exp_read Operation.op_exp_read()
  @exp_modify Operation.op_exp_modify()
  @append Operation.op_append()
  @prepend Operation.op_prepend()
  @touch Operation.op_touch()
  @bit_read Operation.op_bit_read()
  @bit_modify Operation.op_bit_modify()
  @delete Operation.op_delete()
  @hll_read Operation.op_hll_read()
  @hll_modify Operation.op_hll_modify()

  @write_op_types [
    @write,
    @cdt_modify,
    @add,
    @exp_modify,
    @append,
    @prepend,
    @touch,
    @bit_modify,
    @delete,
    @hll_modify
  ]

  @doc false
  @spec scan_ops([Operation.t()]) :: t()
  def scan_ops(ops) when is_list(ops) do
    st =
      Enum.reduce(ops, initial_acc(), fn op, acc ->
        acc
        |> maybe_mark_respond_all(op)
        |> accumulate_by_op_type(op)
      end)

    Map.put(st, :header_only?, st.read_header? and not st.read_bin?)
  end

  defp initial_acc do
    %{
      info1: 0,
      info2: 0,
      info3: 0,
      read_bin?: false,
      read_header?: false,
      respond_all?: false,
      has_write?: false
    }
  end

  defp maybe_mark_respond_all(acc, %Operation{op_type: t, map_cdt: m}) do
    ra? = respond_all_ops_type?(t, m)
    %{acc | respond_all?: acc.respond_all? or ra?}
  end

  # Operation types that imply per-op responses in an `operate` batch (RESPOND_ALL_OPS).
  defp respond_all_ops_type?(t, map_cdt?) do
    t in [@exp_read, @exp_modify, @bit_read, @bit_modify, @hll_read, @hll_modify] or
      (t == @cdt_read and map_cdt?) or
      (t == @cdt_modify and map_cdt?)
  end

  defp accumulate_by_op_type(acc, %Operation{op_type: t, read_header: true}) when t == @read do
    %{acc | info1: acc.info1 ||| AsmMsg.info1_read(), read_header?: true}
  end

  defp accumulate_by_op_type(acc, %Operation{op_type: t} = op) when t == @read do
    info1 = acc.info1 ||| AsmMsg.info1_read()
    info1 = if op.bin_name == "", do: info1 ||| AsmMsg.info1_get_all(), else: info1
    %{acc | info1: info1, read_bin?: true}
  end

  defp accumulate_by_op_type(acc, %Operation{op_type: t}) when t in [@cdt_read, @exp_read] do
    %{acc | info1: acc.info1 ||| AsmMsg.info1_read(), read_bin?: true}
  end

  defp accumulate_by_op_type(acc, %Operation{op_type: t}) when t in [@bit_read, @hll_read] do
    %{acc | info1: acc.info1 ||| AsmMsg.info1_read(), read_bin?: true}
  end

  defp accumulate_by_op_type(acc, %Operation{op_type: t}) when t in @write_op_types do
    %{acc | has_write?: true}
  end

  defp accumulate_by_op_type(acc, _), do: acc
end
