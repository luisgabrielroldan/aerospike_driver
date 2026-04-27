defmodule Aerospike.Exp.Module do
  @moduledoc false

  import Bitwise

  alias Aerospike.Exp
  alias Aerospike.Protocol.Exp, as: Encoder

  @cdt_module 0
  @bit_module 1
  @hll_module 2
  @modify 0x40

  @spec cdt_read(Exp.t(), Exp.exp_type(), integer(), list()) :: Exp.t()
  def cdt_read(%Exp{} = bin, return_type, op_code, args) do
    call(bin, return_type, @cdt_module, op_code, args)
  end

  @spec cdt_modify(Exp.t(), Exp.exp_type(), integer(), list(), Exp.exp_type()) :: Exp.t()
  def cdt_modify(%Exp{} = bin, return_type, op_code, args, _root_type) do
    call(bin, return_type, @cdt_module ||| @modify, op_code, args)
  end

  @spec bit_read(Exp.t(), Exp.exp_type(), integer(), list()) :: Exp.t()
  def bit_read(%Exp{} = bin, return_type, op_code, args) do
    call(bin, return_type, @bit_module, op_code, args)
  end

  @spec bit_modify(Exp.t(), integer(), list()) :: Exp.t()
  def bit_modify(%Exp{} = bin, op_code, args) do
    call(bin, :blob, @bit_module ||| @modify, op_code, args)
  end

  @spec hll_read(Exp.t(), Exp.exp_type(), integer(), list()) :: Exp.t()
  def hll_read(%Exp{} = bin, return_type, op_code, args) do
    call(bin, return_type, @hll_module, op_code, args)
  end

  @spec hll_modify(Exp.t(), integer(), list()) :: Exp.t()
  def hll_modify(%Exp{} = bin, op_code, args) do
    call(bin, :hll, @hll_module ||| @modify, op_code, args)
  end

  defp call(%Exp{} = bin, return_type, module, op_code, args) do
    payload = Encoder.module_payload(op_code, Enum.map(args, &arg/1))

    Exp.from_wire(
      Encoder.encode(%{
        cmd: :call,
        type: return_type,
        module: module,
        payload: payload,
        bin: arg(bin)
      })
    )
  end

  defp arg(%Exp{wire: wire}), do: %{bytes: wire}
  defp arg(other), do: other
end
