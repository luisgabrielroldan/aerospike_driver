defmodule Demo.Examples.BitOps do
  @moduledoc """
  Demonstrates bitwise CDT operations via `Aerospike.Op.Bit`:
  set, get, count, set_int, get_int, and bitwise OR.
  """

  require Logger

  alias Aerospike.Op

  @repo Demo.PrimaryClusterRepo
  @namespace "test"
  @set "demo_bitops"

  def run do
    set_and_get()
    count_bits()
    integer_operations()
    bitwise_or()
    cleanup()
  end

  defp set_and_get do
    key = key("sg")
    @repo.delete(key)
    :ok = @repo.put!(key, %{"bits" => {:blob, <<0::32>>}})

    @repo.operate!(key, [Op.Bit.set("bits", 0, 8, <<0xFF>>)])

    rec = @repo.operate!(key, [Op.Bit.get("bits", 0, 32)])

    {:blob, bytes} = rec.bins["bits"]
    Logger.info("  Set first 8 bits to 0xFF: #{inspect(bytes, base: :hex)}")

    <<first_byte, _rest::binary>> = bytes
    unless first_byte == 0xFF, do: raise("Expected first byte 0xFF, got #{first_byte}")
  end

  defp count_bits do
    key = key("cnt")
    @repo.delete(key)
    :ok = @repo.put!(key, %{"flags" => {:blob, <<0b10101010, 0b11110000>>}})

    rec =
      @repo.operate!(key, [
        Op.Bit.count("flags", 0, 16)
      ])

    count = rec.bins["flags"]
    Logger.info("  Bit count in 0xAAF0: #{count}")
    unless count == 8, do: raise("Expected 8 bits set, got #{count}")
  end

  defp integer_operations do
    key = key("int")
    @repo.delete(key)
    :ok = @repo.put!(key, %{"data" => {:blob, <<0::64>>}})

    @repo.operate!(key, [Op.Bit.set_int("data", 0, 32, 42)])

    rec = @repo.operate!(key, [Op.Bit.get_int("data", 0, 32)])

    val = rec.bins["data"]
    Logger.info("  set_int(42) then get_int: #{val}")
    unless val == 42, do: raise("Expected 42, got #{val}")
  end

  defp bitwise_or do
    key = key("bor")
    @repo.delete(key)
    :ok = @repo.put!(key, %{"mask" => {:blob, <<0b10100000>>}})

    @repo.operate!(key, [Op.Bit.bw_or("mask", 0, 8, <<0b01010000>>)])

    rec = @repo.operate!(key, [Op.Bit.get("mask", 0, 8)])

    {:blob, result} = rec.bins["mask"]
    <<byte>> = result
    Logger.info("  0xA0 OR 0x50 = 0x#{Integer.to_string(byte, 16) |> String.pad_leading(2, "0")}")
    unless byte == 0xF0, do: raise("Expected 0xF0, got #{byte}")
  end

  defp cleanup do
    for id <- ["sg", "cnt", "int", "bor"] do
      @repo.delete(key(id))
    end
  end

  defp key(id), do: Aerospike.key(@namespace, @set, id)
end
