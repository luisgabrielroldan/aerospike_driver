defmodule Aerospike.Protocol.CDT do
  @moduledoc false

  # Shared MessagePack payload builders for list/map/bit/HLL CDT operations.

  alias Aerospike.Protocol.AsmMsg.Operation
  alias Aerospike.Protocol.MessagePack

  # CDT operation values are MessagePack in a bin; wire particle type is BLOB (4), matching
  # Go/Java clients (`ParticleType.BLOB` for packed CDT payloads).
  @particle_blob 4

  @context_eval 0xFF

  @doc """
  List or generic CDT read: `op_type` wire byte 3, payload MessagePack blob.
  """
  @spec list_read_op(String.t(), integer(), [term()], [Aerospike.Ctx.step()] | nil) ::
          Operation.t()
  def list_read_op(bin_name, op_code, args, ctx \\ nil)
      when is_binary(bin_name) and is_integer(op_code) and is_list(args) do
    %Operation{
      op_type: Operation.op_cdt_read(),
      particle_type: @particle_blob,
      bin_name: bin_name,
      data: encode_payload(op_code, args, ctx)
    }
  end

  @doc """
  List or generic CDT modify: `op_type` wire byte 4.
  """
  @spec list_modify_op(String.t(), integer(), [term()], [Aerospike.Ctx.step()] | nil) ::
          Operation.t()
  def list_modify_op(bin_name, op_code, args, ctx \\ nil)
      when is_binary(bin_name) and is_integer(op_code) and is_list(args) do
    %Operation{
      op_type: Operation.op_cdt_modify(),
      particle_type: @particle_blob,
      bin_name: bin_name,
      data: encode_payload(op_code, args, ctx)
    }
  end

  @doc """
  Map CDT read — same wire op bytes as list CDT (3), different opcode namespace (64+).
  """
  @spec map_read_op(String.t(), integer(), [term()], [Aerospike.Ctx.step()] | nil) ::
          Operation.t()
  def map_read_op(bin_name, op_code, args, ctx \\ nil)
      when is_binary(bin_name) and is_integer(op_code) and is_list(args) do
    # Go uses `_MAP_READ` for operate flag routing; wire `.op` is still 3.
    %{list_read_op(bin_name, op_code, args, ctx) | map_cdt: true}
  end

  @doc """
  Map CDT modify — wire op byte 4.
  """
  @spec map_modify_op(String.t(), integer(), [term()], [Aerospike.Ctx.step()] | nil) ::
          Operation.t()
  def map_modify_op(bin_name, op_code, args, ctx \\ nil)
      when is_binary(bin_name) and is_integer(op_code) and is_list(args) do
    %{list_modify_op(bin_name, op_code, args, ctx) | map_cdt: true}
  end

  @doc "Bit read (wire op 12)."
  @spec bit_read_op(String.t(), integer(), [term()], [Aerospike.Ctx.step()] | nil) ::
          Operation.t()
  def bit_read_op(bin_name, op_code, args, ctx \\ nil)
      when is_binary(bin_name) and is_integer(op_code) and is_list(args) do
    %Operation{
      op_type: Operation.op_bit_read(),
      particle_type: @particle_blob,
      bin_name: bin_name,
      data: encode_payload(op_code, args, ctx)
    }
  end

  @doc "Bit modify (wire op 13)."
  @spec bit_modify_op(String.t(), integer(), [term()], [Aerospike.Ctx.step()] | nil) ::
          Operation.t()
  def bit_modify_op(bin_name, op_code, args, ctx \\ nil)
      when is_binary(bin_name) and is_integer(op_code) and is_list(args) do
    %Operation{
      op_type: Operation.op_bit_modify(),
      particle_type: @particle_blob,
      bin_name: bin_name,
      data: encode_payload(op_code, args, ctx)
    }
  end

  @doc "HLL read (wire op 15); server does not support nested HLL context."
  @spec hll_read_op(String.t(), integer(), [term()]) :: Operation.t()
  def hll_read_op(bin_name, op_code, args)
      when is_binary(bin_name) and is_integer(op_code) and is_list(args) do
    %Operation{
      op_type: Operation.op_hll_read(),
      particle_type: @particle_blob,
      bin_name: bin_name,
      data: encode_simple_array(op_code, args)
    }
  end

  @doc "HLL modify (wire op 16)."
  @spec hll_modify_op(String.t(), integer(), [term()]) :: Operation.t()
  def hll_modify_op(bin_name, op_code, args)
      when is_binary(bin_name) and is_integer(op_code) and is_list(args) do
    %Operation{
      op_type: Operation.op_hll_modify(),
      particle_type: @particle_blob,
      bin_name: bin_name,
      data: encode_simple_array(op_code, args)
    }
  end

  @doc false
  @spec cdt_pack_arg(term()) :: term()
  def cdt_pack_arg({:bytes, _} = b), do: b

  # UTF-8 string arguments must use Aerospike's particle-wrapped encoding inside
  # CDT blobs (not plain MessagePack strings). See `MessagePack` `:particle_string`.
  def cdt_pack_arg(s) when is_binary(s), do: {:particle_string, s}

  def cdt_pack_arg(list) when is_list(list) do
    Enum.map(list, &cdt_pack_arg/1)
  end

  def cdt_pack_arg(map) when is_map(map) do
    Map.new(map, fn {k, v} -> {cdt_pack_arg(k), cdt_pack_arg(v)} end)
  end

  def cdt_pack_arg(other), do: other

  defp encode_simple_array(op_code, args) do
    MessagePack.pack!([op_code | Enum.map(args, &cdt_pack_arg/1)])
  end

  defp encode_payload(op_code, args, ctx) do
    inner = [op_code | Enum.map(args, &cdt_pack_arg/1)]

    case ctx do
      nil ->
        MessagePack.pack!(inner)

      [] ->
        MessagePack.pack!(inner)

      steps when is_list(steps) ->
        ctx_elems =
          Enum.flat_map(steps, fn {id, val} ->
            [id, cdt_pack_arg(val)]
          end)

        MessagePack.pack!([@context_eval, ctx_elems, inner])
    end
  end
end
