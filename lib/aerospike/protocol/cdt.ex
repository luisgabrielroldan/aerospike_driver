defmodule Aerospike.Protocol.CDT do
  @moduledoc false

  alias Aerospike.Exp
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

  @spec list_create_op(String.t(), integer(), [term()], [tuple()]) :: Operation.t()
  def list_create_op(bin_name, flag, args, ctx)
      when is_binary(bin_name) and is_integer(flag) and is_list(args) and is_list(ctx) do
    %Operation{
      op_type: Operation.op_cdt_modify(),
      particle_type: @particle_blob,
      bin_name: bin_name,
      data: encode_create_payload(flag, args, ctx)
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

  @spec map_create_op(String.t(), integer(), [term()], [tuple()]) :: Operation.t()
  def map_create_op(bin_name, flag, args, ctx)
      when is_binary(bin_name) and is_integer(flag) and is_list(args) and is_list(ctx) do
    %{list_create_op(bin_name, flag, args, ctx) | map_cdt: true}
  end

  @spec bit_read_op(String.t(), integer(), [term()], [tuple()] | nil) :: Operation.t()
  def bit_read_op(bin_name, op_code, args, ctx \\ nil)
      when is_binary(bin_name) and is_integer(op_code) and is_list(args) do
    %Operation{
      op_type: Operation.op_bit_read(),
      particle_type: @particle_blob,
      bin_name: bin_name,
      data: encode_payload(op_code, args, ctx)
    }
  end

  @spec bit_modify_op(String.t(), integer(), [term()], [tuple()] | nil) :: Operation.t()
  def bit_modify_op(bin_name, op_code, args, ctx \\ nil)
      when is_binary(bin_name) and is_integer(op_code) and is_list(args) do
    %Operation{
      op_type: Operation.op_bit_modify(),
      particle_type: @particle_blob,
      bin_name: bin_name,
      data: encode_payload(op_code, args, ctx)
    }
  end

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

  @spec cdt_pack_arg(term()) :: term()
  def cdt_pack_arg(%Exp{wire: wire}), do: {:byte_array, wire}
  def cdt_pack_arg({:bytes, _} = bytes), do: bytes
  def cdt_pack_arg(s) when is_binary(s), do: {:particle_string, s}
  def cdt_pack_arg(list) when is_list(list), do: Enum.map(list, &cdt_pack_arg/1)

  def cdt_pack_arg(map) when is_map(map),
    do: Map.new(map, fn {k, v} -> {cdt_pack_arg(k), cdt_pack_arg(v)} end)

  def cdt_pack_arg(other), do: other

  @spec encode_ctx([Aerospike.Ctx.step()]) :: binary()
  def encode_ctx(ctx) when is_list(ctx) do
    ctx
    |> ctx_terms()
    |> MessagePack.pack!()
  end

  defp encode_payload(op_code, args, ctx) do
    inner = [op_code | Enum.map(args, &cdt_pack_arg/1)]

    case ctx do
      nil ->
        MessagePack.pack!(inner)

      [] ->
        MessagePack.pack!(inner)

      steps when is_list(steps) ->
        MessagePack.pack!([@context_eval, ctx_terms(steps), inner])
    end
  end

  defp encode_create_payload(flag, args, ctx) do
    [last | reversed] = Enum.reverse(ctx)

    ctx =
      reversed
      |> Enum.reverse()
      |> Kernel.++([apply_create_flag(last, flag)])

    MessagePack.pack!([@context_eval, ctx_terms(ctx), Enum.map(args, &cdt_pack_arg/1)])
  end

  defp ctx_terms(ctx) do
    Enum.flat_map(ctx, fn {id, val} -> [id, cdt_pack_arg(val)] end)
  end

  defp apply_create_flag({id, val}, flag), do: {Bitwise.bor(id, flag), val}

  defp encode_simple_array(op_code, args) do
    MessagePack.pack!([op_code | Enum.map(args, &cdt_pack_arg/1)])
  end
end
