defmodule Aerospike.Protocol.Admin do
  @moduledoc false

  alias Aerospike.Protocol.Message
  alias Aerospike.Protocol.ResultCode

  # Admin command IDs
  @login 20

  # Field IDs
  @user 0
  @credential 3

  @msg_version 2
  @msg_type 2

  @doc """
  Encodes an internal-auth LOGIN request: user + credential blob.

  The `credential` binary must match what the server expects (bcrypt hash of the password).
  TODO: expose a helper for password hashing.

  ## Examples

      iex> bin = Aerospike.Protocol.Admin.encode_login("u", <<1, 2, 3>>)
      iex> byte_size(bin) > 24
      true

  """
  @spec encode_login(String.t(), binary()) :: binary()
  def encode_login(user, credential) when is_binary(credential) do
    # 16-byte admin header after the 8-byte message header
    admin_block = <<0, 0, @login, 2>> <> <<0::size(96)>>
    user_field = encode_field(@user, user)
    cred_field = encode_field(@credential, credential)
    payload = admin_block <> user_field <> cred_field
    Message.encode(@msg_version, @msg_type, payload)
  end

  defp encode_field(id, data) when is_binary(data) do
    len = byte_size(data)
    <<len + 1::32-big, id::8, data::binary>>
  end

  @doc """
  Decodes an admin response frame (8-byte message header + body).

  Returns the mapped result code atom and the raw integer from the wire.
  """
  @spec decode_response(binary()) ::
          {:ok, %{result_code: atom(), raw: non_neg_integer()}}
          | {:error, atom()}
  def decode_response(data) when is_binary(data) do
    with {:ok, {_version, _type, body}} <- Message.decode(data) do
      decode_admin_body(body)
    end
  end

  @doc """
  Decodes result code from an admin message body (payload after the 8-byte message header).

  The result code is at byte index 1 of the body (offset 9 in the full 24-byte frame:
  8-byte framing + 16-byte admin body).
  """
  @spec decode_admin_body(binary()) ::
          {:ok, %{result_code: atom(), raw: non_neg_integer()}}
          | {:error, atom()}
  def decode_admin_body(<<_::8, raw_rc::8, _::binary>>) do
    case ResultCode.from_integer(raw_rc) do
      {:ok, atom} -> {:ok, %{result_code: atom, raw: raw_rc}}
      {:error, code} -> {:ok, %{result_code: :unknown_result_code, raw: code}}
    end
  end

  def decode_admin_body(body) when is_binary(body), do: {:error, :short_body}

  @doc """
  Parses session token and TTL from the tail of a LOGIN response body (after the first 16 bytes),
  when the server returns session fields.
  """
  @spec decode_login_session_fields(binary()) :: %{
          session_token: binary() | nil,
          session_ttl_sec: non_neg_integer() | nil
        }
  def decode_login_session_fields(body) when byte_size(body) <= 16,
    do: %{session_token: nil, session_ttl_sec: nil}

  def decode_login_session_fields(body) do
    extra = binary_part(body, 16, byte_size(body) - 16)
    parse_session_fields(extra, %{session_token: nil, session_ttl_sec: nil})
  end

  defp parse_session_fields(<<>>, acc), do: acc

  defp parse_session_fields(<<flen::32-big, id::8, rest::binary>>, acc) do
    mlen = flen - 1

    case rest do
      <<field_data::binary-size(mlen), tail::binary>> ->
        acc2 = apply_session_field(id, field_data, acc)
        parse_session_fields(tail, acc2)

      _ ->
        acc
    end
  end

  defp parse_session_fields(_other, acc), do: acc

  defp apply_session_field(5, field_data, acc), do: %{acc | session_token: field_data}

  defp apply_session_field(6, field_data, acc),
    do: %{acc | session_ttl_sec: decode_uint32(field_data)}

  defp apply_session_field(_, _, acc), do: acc

  defp decode_uint32(<<n::32-big>>), do: n
  defp decode_uint32(_), do: nil

  @doc false
  def login_command_id, do: @login
end
