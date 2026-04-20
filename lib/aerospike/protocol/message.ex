defmodule Aerospike.Protocol.Message do
  @moduledoc false

  import Bitwise

  @proto_version 2
  @type_info 1
  @type_as_msg 3
  @type_compressed 4

  @doc """
  Returns the protocol version (always 2).
  """
  @spec proto_version() :: 2
  def proto_version, do: @proto_version

  @doc """
  Returns the message type constant for info messages.
  """
  @spec type_info() :: 1
  def type_info, do: @type_info

  @doc """
  Returns the message type constant for AS_MSG messages.
  """
  @spec type_as_msg() :: 3
  def type_as_msg, do: @type_as_msg

  @doc """
  Returns the message type constant for compressed messages.
  """
  @spec type_compressed() :: 4
  def type_compressed, do: @type_compressed

  @doc """
  Encodes a complete message with the 8-byte protocol header and payload.

  The protocol header is a big-endian 64-bit word:
  - Bits 56-63: version (always 2)
  - Bits 48-55: type (1=info, 3=AS_MSG, 4=compressed)
  - Bits 0-47: payload length (excludes the 8-byte header)

  ## Examples

      iex> Aerospike.Protocol.Message.encode(2, 1, "test")
      <<2, 1, 0, 0, 0, 0, 0, 4, "test">>

  """
  @spec encode(non_neg_integer(), non_neg_integer(), binary()) :: binary()
  def encode(version, type, payload) when is_binary(payload) do
    encode_header(version, type, byte_size(payload)) <> payload
  end

  @doc """
  Encodes just the 8-byte protocol header.

  ## Examples

      iex> Aerospike.Protocol.Message.encode_header(2, 3, 100)
      <<2, 3, 0, 0, 0, 0, 0, 100>>

  """
  @spec encode_header(non_neg_integer(), non_neg_integer(), non_neg_integer()) :: <<_::64>>
  def encode_header(version, type, length) do
    proto_word = length ||| version <<< 56 ||| type <<< 48
    <<proto_word::64-big>>
  end

  @doc """
  Decodes an 8-byte protocol header into {version, type, length}.

  ## Examples

      iex> Aerospike.Protocol.Message.decode_header(<<2, 3, 0, 0, 0, 0, 0, 100>>)
      {:ok, {2, 3, 100}}

      iex> Aerospike.Protocol.Message.decode_header(<<1, 2, 3>>)
      {:error, :incomplete_header}

  """
  @spec decode_header(binary()) ::
          {:ok, {non_neg_integer(), non_neg_integer(), non_neg_integer()}}
          | {:error, :incomplete_header}
  def decode_header(<<proto_word::64-big>>) do
    version = proto_word >>> 56 &&& 0xFF
    type = proto_word >>> 48 &&& 0xFF
    length = proto_word &&& 0x0000FFFFFFFFFFFF
    {:ok, {version, type, length}}
  end

  def decode_header(_), do: {:error, :incomplete_header}

  @doc """
  Decodes a complete message (header + payload) into {version, type, body}.

  Returns an error if the binary is incomplete or malformed.

  ## Examples

      iex> Aerospike.Protocol.Message.decode(<<2, 1, 0, 0, 0, 0, 0, 4, "test">>)
      {:ok, {2, 1, "test"}}

      iex> Aerospike.Protocol.Message.decode(<<2, 1, 0, 0, 0, 0, 0, 10, "short">>)
      {:error, :incomplete_body}

  """
  @spec decode(binary()) ::
          {:ok, {non_neg_integer(), non_neg_integer(), binary()}}
          | {:error, :incomplete_header | :incomplete_body}
  def decode(<<header::binary-8, rest::binary>>) do
    with {:ok, {version, type, length}} <- decode_header(header) do
      if byte_size(rest) >= length do
        <<body::binary-size(length), _rest::binary>> = rest
        {:ok, {version, type, body}}
      else
        {:error, :incomplete_body}
      end
    end
  end

  def decode(_), do: {:error, :incomplete_header}

  @doc """
  Encodes an info message (type 1) with the default protocol version.

  ## Examples

      iex> Aerospike.Protocol.Message.encode_info("namespaces\\n")
      <<2, 1, 0, 0, 0, 0, 0, 11, "namespaces\\n">>

  """
  @spec encode_info(binary()) :: binary()
  def encode_info(payload), do: encode(@proto_version, @type_info, payload)

  @doc """
  Encodes an AS_MSG message (type 3) with the default protocol version.

  ## Examples

      iex> Aerospike.Protocol.Message.encode_as_msg(<<22, 0, 0, 0>>)
      <<2, 3, 0, 0, 0, 0, 0, 4, 22, 0, 0, 0>>

  """
  @spec encode_as_msg(binary()) :: binary()
  def encode_as_msg(payload), do: encode(@proto_version, @type_as_msg, payload)

  @doc """
  Encodes an AS_MSG message (type 3) from an iodata payload, returning iodata.

  Computes the payload length via `IO.iodata_length/1` and prepends the 8-byte
  protocol header. The result is iodata (a list), not a flat binary.
  """
  @spec encode_as_msg_iodata(iodata()) :: iodata()
  def encode_as_msg_iodata(payload) do
    length = IO.iodata_length(payload)
    [encode_header(@proto_version, @type_as_msg, length) | payload]
  end
end
