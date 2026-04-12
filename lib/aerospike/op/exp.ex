defmodule Aerospike.Op.Exp do
  @moduledoc """
  Expression operations for use with `Aerospike.operate/4`.

  Expression operations evaluate a server-side expression and either return the
  result in the response record's bins (`read/3`) or write the result to a bin
  (`write/3`).

  ## Usage

      alias Aerospike.Exp
      alias Aerospike.Op

      {:ok, record} =
        MyApp.Repo.operate(key, [
          Op.Exp.read("age_plus_ten", Exp.add(Exp.int_bin("age"), Exp.int(10))),
          Op.Exp.write("doubled", Exp.mul(Exp.int_bin("count"), Exp.int(2)))
        ])

  ## Related

  - `Aerospike.Exp` — expression builder
  - `MyApp.Repo.operate/2,3` — recommended application-facing execution path
  - `Aerospike.operate/4` — low-level atomic execution on one record
  """

  alias Aerospike.Exp
  alias Aerospike.Protocol.AsmMsg.Operation
  alias Aerospike.Protocol.MessagePack

  @typedoc """
  Opaque expression operation for `Aerospike.operate/4`.

  Construct with `read/3` or `write/3` and pass in an operation list.
  """
  @opaque t :: Aerospike.Op.t()

  @doc """
  Reads the result of a server-side expression into `bin_name`.

  The expression is evaluated on the server and the result appears in the
  `bins` map of the returned record under `bin_name`.

  Accepts a `flags:` integer option (default `0`).
  """
  @spec read(String.t(), Exp.t(), keyword()) :: t()
  def read(bin_name, %Exp{wire: w}, opts \\ []) when is_binary(bin_name) do
    flags = Keyword.get(opts, :flags, 0)

    %Operation{
      op_type: Operation.op_exp_read(),
      particle_type: Operation.particle_blob(),
      bin_name: bin_name,
      data: encode_exp_op(w, flags)
    }
  end

  @doc """
  Writes the result of a server-side expression to `bin_name`.

  Accepts a `flags:` integer option (default `0`).
  """
  @spec write(String.t(), Exp.t(), keyword()) :: t()
  def write(bin_name, %Exp{wire: w}, opts \\ []) when is_binary(bin_name) do
    flags = Keyword.get(opts, :flags, 0)

    %Operation{
      op_type: Operation.op_exp_modify(),
      particle_type: Operation.particle_blob(),
      bin_name: bin_name,
      data: encode_exp_op(w, flags)
    }
  end

  # Encodes the exp operation payload: a 2-element msgpack array [exp_wire, flags].
  # The expression wire bytes are already valid msgpack — written directly into the
  # fixarray without re-wrapping. 0x92 is the msgpack fixarray header for 2 elements.
  defp encode_exp_op(wire, flags) when is_binary(wire) and is_integer(flags) do
    <<0x92>> <> wire <> MessagePack.pack!(flags)
  end
end
