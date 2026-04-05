defmodule Aerospike.Protocol.MessagePack do
  @moduledoc false

  # Minimal MessagePack encode/decode for CDT wire payloads (arrays, scalars,
  # nested lists/maps). Ext types decode to `{:ext, type_byte, data}`.
  #
  # CDT operation blobs represent Aerospike string values as MessagePack strings
  # whose payload is `<<particle_type_string, utf8_bytes::binary>>` (see
  # `{:particle_string, binary}`), matching official clients — not as raw UTF-8
  # MessagePack strings.

  @particle_string 3
  @particle_blob 4

  @spec pack!(term()) :: binary()
  def pack!(term), do: IO.iodata_to_binary(pack_iolist(term))

  defp pack_iolist(nil), do: [<<0xC0>>]

  defp pack_iolist(false), do: [<<0xC2>>]
  defp pack_iolist(true), do: [<<0xC3>>]

  defp pack_iolist(n) when is_integer(n) and n >= 0 do
    cond do
      n <= 127 -> [<<n::8>>]
      n <= 0xFF -> [<<0xCC, n::8>>]
      n <= 0xFFFF -> [<<0xCD, n::16-big>>]
      n <= 0xFFFF_FFFF -> [<<0xCE, n::32-big>>]
      true -> [<<0xCF, n::64-big>>]
    end
  end

  defp pack_iolist(n) when is_integer(n) do
    cond do
      n >= -32 and n <= -1 ->
        # Negative fixint: 0xE0..0xFF
        [<<256 + n::8>>]

      n >= -128 ->
        [<<0xD0, n::8-signed>>]

      n >= -0x8000 ->
        [<<0xD1, n::16-signed-big>>]

      n >= -0x8000_0000 ->
        [<<0xD2, n::32-signed-big>>]

      true ->
        [<<0xD3, n::64-signed-big>>]
    end
  end

  defp pack_iolist(f) when is_float(f), do: [<<0xCB, f::64-float-big>>]

  # Aerospike Value.STRING (particle type 3) wrapped in a MessagePack str, used
  # inside CDT / HLL packed payloads (official clients: packParticleString).
  defp pack_iolist({:particle_string, utf8}) when is_binary(utf8) do
    inner = <<@particle_string, utf8::binary>>
    [string_prefix(byte_size(inner)), inner]
  end

  # Aerospike Value.BLOB in CDT args: MessagePack *str* (not bin) with payload
  # `<<particle_blob, bytes>>` — same as Go `packBytes` / Java `packParticleBytes`.
  defp pack_iolist({:bytes, bin}) when is_binary(bin) do
    inner = <<@particle_blob, bin::binary>>
    [string_prefix(byte_size(inner)), inner]
  end

  defp pack_iolist(bin) when is_binary(bin) do
    [string_prefix(byte_size(bin)), bin]
  end

  defp pack_iolist(list) when is_list(list) do
    len = length(list)
    [array_header(len) | Enum.map(list, &pack_iolist/1)]
  end

  defp pack_iolist(%{} = map) do
    pairs = Map.to_list(map)
    [map_header(length(pairs)) | map_pair_iodata(pairs)]
  end

  defp pack_iolist({:ext, type, data}) when is_integer(type) and is_binary(data) do
    [ext_header(byte_size(data), type), data]
  end

  defp pack_iolist(other) do
    raise ArgumentError, "MessagePack.pack!: unsupported term #{inspect(other)}"
  end

  defp array_header(len) when len <= 15, do: <<0x90 + len>>
  defp array_header(len) when len <= 0xFFFF, do: <<0xDC, len::16-big>>
  defp array_header(len), do: <<0xDD, len::32-big>>

  defp map_header(len) when len <= 15, do: <<0x80 + len>>
  defp map_header(len) when len <= 0xFFFF, do: <<0xDE, len::16-big>>
  defp map_header(len), do: <<0xDF, len::32-big>>

  defp map_pair_iodata(pairs) do
    Enum.flat_map(pairs, fn {k, v} -> [pack_iolist(k), pack_iolist(v)] end)
  end

  defp ext_header(1, type), do: <<0xD4, type::8>>
  defp ext_header(2, type), do: <<0xD5, type::8>>
  defp ext_header(4, type), do: <<0xD6, type::8>>
  defp ext_header(8, type), do: <<0xD7, type::8>>
  defp ext_header(16, type), do: <<0xD8, type::8>>
  defp ext_header(dlen, type) when dlen <= 0xFF, do: <<0xC7, dlen::8, type::8>>
  defp ext_header(dlen, type) when dlen <= 0xFFFF, do: <<0xC8, dlen::16-big, type::8>>
  defp ext_header(dlen, type), do: <<0xC9, dlen::32-big, type::8>>

  defp string_prefix(len) when len <= 31, do: <<0xA0 + len>>
  defp string_prefix(len) when len <= 0xFF, do: <<0xD9, len::8>>
  defp string_prefix(len) when len <= 0xFFFF, do: <<0xDA, len::16-big>>
  defp string_prefix(len), do: <<0xDB, len::32-big>>

  @spec unpack!(binary()) :: term()
  def unpack!(bin) when is_binary(bin) do
    case unpack(bin) do
      {:ok, {term, <<>>}} ->
        term

      {:ok, {_term, rest}} ->
        raise ArgumentError, "MessagePack.unpack!: trailing bytes (#{byte_size(rest)})"

      {:error, :invalid_msgpack} ->
        raise ArgumentError, "MessagePack.unpack!: incomplete or invalid input"
    end
  end

  @spec unpack(binary()) :: {:ok, {term(), binary()}} | {:error, :invalid_msgpack}
  def unpack(bin) when is_binary(bin), do: do_unpack(bin)

  defp do_unpack(<<0xC0, rest::binary>>), do: {:ok, {nil, rest}}

  defp do_unpack(<<0xC2, rest::binary>>), do: {:ok, {false, rest}}
  defp do_unpack(<<0xC3, rest::binary>>), do: {:ok, {true, rest}}

  defp do_unpack(<<b, rest::binary>>) when b <= 0x7F, do: {:ok, {b, rest}}

  defp do_unpack(<<b, rest::binary>>) when b >= 0xE0 do
    {:ok, {b - 256, rest}}
  end

  defp do_unpack(<<0xCC, n::8, rest::binary>>), do: {:ok, {n, rest}}
  defp do_unpack(<<0xCD, n::16-big, rest::binary>>), do: {:ok, {n, rest}}
  defp do_unpack(<<0xCE, n::32-big, rest::binary>>), do: {:ok, {n, rest}}
  defp do_unpack(<<0xCF, n::64-big, rest::binary>>), do: {:ok, {n, rest}}

  defp do_unpack(<<0xD0, n::8-signed, rest::binary>>), do: {:ok, {n, rest}}
  defp do_unpack(<<0xD1, n::16-signed-big, rest::binary>>), do: {:ok, {n, rest}}
  defp do_unpack(<<0xD2, n::32-signed-big, rest::binary>>), do: {:ok, {n, rest}}
  defp do_unpack(<<0xD3, n::64-signed-big, rest::binary>>), do: {:ok, {n, rest}}

  defp do_unpack(<<0xCA, f::32-float-big, rest::binary>>), do: {:ok, {f, rest}}
  defp do_unpack(<<0xCB, f::64-float-big, rest::binary>>), do: {:ok, {f, rest}}

  defp do_unpack(<<b, rest::binary>>) when b >= 0xA0 and b <= 0xBF do
    len = b - 0xA0

    if byte_size(rest) >= len do
      <<str::binary-size(len), r::binary>> = rest
      {:ok, {unpack_cdt_string_payload(str), r}}
    else
      {:error, :invalid_msgpack}
    end
  end

  defp do_unpack(<<0xD9, len::8, rest::binary>>) do
    if byte_size(rest) >= len do
      <<str::binary-size(len), r::binary>> = rest
      {:ok, {unpack_cdt_string_payload(str), r}}
    else
      {:error, :invalid_msgpack}
    end
  end

  defp do_unpack(<<0xDA, len::16-big, rest::binary>>) do
    if byte_size(rest) >= len do
      <<str::binary-size(len), r::binary>> = rest
      {:ok, {unpack_cdt_string_payload(str), r}}
    else
      {:error, :invalid_msgpack}
    end
  end

  defp do_unpack(<<0xDB, len::32-big, rest::binary>>) do
    if byte_size(rest) >= len do
      <<str::binary-size(len), r::binary>> = rest
      {:ok, {unpack_cdt_string_payload(str), r}}
    else
      {:error, :invalid_msgpack}
    end
  end

  defp do_unpack(<<0xC4, len::8, rest::binary>>) do
    if byte_size(rest) >= len do
      <<bin::binary-size(len), r::binary>> = rest
      {:ok, {bin, r}}
    else
      {:error, :invalid_msgpack}
    end
  end

  defp do_unpack(<<0xC5, len::16-big, rest::binary>>) do
    if byte_size(rest) >= len do
      <<bin::binary-size(len), r::binary>> = rest
      {:ok, {bin, r}}
    else
      {:error, :invalid_msgpack}
    end
  end

  defp do_unpack(<<0xC6, len::32-big, rest::binary>>) do
    if byte_size(rest) >= len do
      <<bin::binary-size(len), r::binary>> = rest
      {:ok, {bin, r}}
    else
      {:error, :invalid_msgpack}
    end
  end

  defp do_unpack(<<b, rest::binary>>) when b >= 0x90 and b <= 0x9F do
    unpack_list(rest, b - 0x90, [])
  end

  defp do_unpack(<<0xDC, n::16-big, rest::binary>>), do: unpack_list(rest, n, [])
  defp do_unpack(<<0xDD, n::32-big, rest::binary>>), do: unpack_list(rest, n, [])

  defp do_unpack(<<b, rest::binary>>) when b >= 0x80 and b <= 0x8F do
    unpack_map(rest, b - 0x80, %{})
  end

  defp do_unpack(<<0xDE, n::16-big, rest::binary>>), do: unpack_map(rest, n, %{})
  defp do_unpack(<<0xDF, n::32-big, rest::binary>>), do: unpack_map(rest, n, %{})

  defp do_unpack(<<0xD4, type::8, data::binary-size(1), rest::binary>>),
    do: {:ok, {{:ext, type, data}, rest}}

  defp do_unpack(<<0xD5, type::8, data::binary-size(2), rest::binary>>),
    do: {:ok, {{:ext, type, data}, rest}}

  defp do_unpack(<<0xD6, type::8, data::binary-size(4), rest::binary>>),
    do: {:ok, {{:ext, type, data}, rest}}

  defp do_unpack(<<0xD7, type::8, data::binary-size(8), rest::binary>>),
    do: {:ok, {{:ext, type, data}, rest}}

  defp do_unpack(<<0xD8, type::8, data::binary-size(16), rest::binary>>),
    do: {:ok, {{:ext, type, data}, rest}}

  defp do_unpack(<<0xC7, dlen::8, type::8, rest::binary>>) do
    if byte_size(rest) >= dlen do
      <<data::binary-size(dlen), r::binary>> = rest
      {:ok, {{:ext, type, data}, r}}
    else
      {:error, :invalid_msgpack}
    end
  end

  defp do_unpack(<<0xC8, dlen::16-big, type::8, rest::binary>>) do
    if byte_size(rest) >= dlen do
      <<data::binary-size(dlen), r::binary>> = rest
      {:ok, {{:ext, type, data}, r}}
    else
      {:error, :invalid_msgpack}
    end
  end

  defp do_unpack(<<0xC9, dlen::32-big, type::8, rest::binary>>) do
    if byte_size(rest) >= dlen do
      <<data::binary-size(dlen), r::binary>> = rest
      {:ok, {{:ext, type, data}, r}}
    else
      {:error, :invalid_msgpack}
    end
  end

  defp do_unpack(_), do: {:error, :invalid_msgpack}

  defp unpack_list(rest, 0, acc), do: {:ok, {Enum.reverse(acc), rest}}

  defp unpack_list(rest, n, acc) do
    case do_unpack(rest) do
      {:ok, {elem, r}} -> unpack_list(r, n - 1, [elem | acc])
      {:error, _} = err -> err
    end
  end

  defp unpack_map(rest, 0, map), do: {:ok, {map, rest}}

  defp unpack_map(rest, n, map) do
    case do_unpack(rest) do
      {:ok, {k, r1}} ->
        case do_unpack(r1) do
          {:ok, {v, r2}} -> unpack_map(r2, n - 1, Map.put(map, k, v))
          {:error, _} = err -> err
        end

      {:error, _} = err ->
        err
    end
  end

  # CDT uses msgpack str (not bin) for particle-wrapped blob payloads.
  defp unpack_cdt_string_payload(<<@particle_blob, rest::binary>>), do: {:blob, rest}
  defp unpack_cdt_string_payload(bin) when is_binary(bin), do: bin
end
