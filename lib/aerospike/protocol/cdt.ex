defmodule Aerospike.Protocol.CDT do
  @moduledoc false

  alias Aerospike.Protocol.AsmMsg.Operation
  alias Aerospike.Protocol.MessagePack

  @particle_blob 4
  @context_eval 0xFF

  @spec list_read_op(String.t(), integer(), [term()], [tuple()] | nil) :: Operation.t()
  def list_read_op(bin_name, op_code, args, ctx \\ nil)
      when is_binary(bin_name) and is_integer(op_code) and is_list(args) do
    %Operation{
      op_type: Operation.op_cdt_read(),
      particle_type: @particle_blob,
      bin_name: bin_name,
      data: encode_payload(op_code, args, ctx)
    }
  end

  @spec list_modify_op(String.t(), integer(), [term()], [tuple()] | nil) :: Operation.t()
  def list_modify_op(bin_name, op_code, args, ctx \\ nil)
      when is_binary(bin_name) and is_integer(op_code) and is_list(args) do
    %Operation{
      op_type: Operation.op_cdt_modify(),
      particle_type: @particle_blob,
      bin_name: bin_name,
      data: encode_payload(op_code, args, ctx)
    }
  end

  @spec map_read_op(String.t(), integer(), [term()], [tuple()] | nil) :: Operation.t()
  def map_read_op(bin_name, op_code, args, ctx \\ nil)
      when is_binary(bin_name) and is_integer(op_code) and is_list(args) do
    %{list_read_op(bin_name, op_code, args, ctx) | map_cdt: true}
  end

  @spec map_modify_op(String.t(), integer(), [term()], [tuple()] | nil) :: Operation.t()
  def map_modify_op(bin_name, op_code, args, ctx \\ nil)
      when is_binary(bin_name) and is_integer(op_code) and is_list(args) do
    %{list_modify_op(bin_name, op_code, args, ctx) | map_cdt: true}
  end

  @spec cdt_pack_arg(term()) :: term()
  def cdt_pack_arg({:bytes, _} = bytes), do: bytes
  def cdt_pack_arg(s) when is_binary(s), do: {:particle_string, s}
  def cdt_pack_arg(list) when is_list(list), do: Enum.map(list, &cdt_pack_arg/1)

  def cdt_pack_arg(map) when is_map(map),
    do: Map.new(map, fn {k, v} -> {cdt_pack_arg(k), cdt_pack_arg(v)} end)

  def cdt_pack_arg(other), do: other

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
