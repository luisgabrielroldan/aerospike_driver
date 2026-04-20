defmodule Aerospike.Protocol.Login do
  @moduledoc """
  Wire encoder/decoder for Aerospike's admin-protocol login and authenticate
  commands.

  Admin frames use a dedicated proto type (`2`) distinct from info (`1`) and
  AS_MSG (`3`). A login request is a 24-byte header (8-byte proto + 16-byte
  admin header) followed by zero or more typed fields:

      +--------+--------+--------+--------+--------+--------+--------+--------+
      | version|  type  |                 length (6 bytes)                    |
      | =  2   |  = 2   |                                                     |
      +--------+--------+--------+--------+--------+--------+--------+--------+
      |  0     |  0     | command|  fc    |           padding (12 bytes)      |
      |        |        |        |        |                                   |
      +--------+--------+--------+--------+-----------------------------------+
      |                             fields...                                 |

  Each field is 4-byte BE `uint32 = len(value) + 1` followed by a 1-byte
  field id followed by `len(value)` value bytes. The 16-byte admin header
  reserves bytes 2 and 3 for command id and field count respectively; the
  remaining 14 bytes are zero-filled.

  Replies carry the same frame shape. Byte 9 of the reply (offset 1 of the
  admin header) is the result code: `0` on success, `52`
  (`:security_not_enabled`) when the server has security disabled, and any
  other code flows through `Aerospike.Protocol.ResultCode.from_integer/1`.
  On a zero result code a populated reply contains `_SESSION_TOKEN` (id 5)
  and optionally `_SESSION_TTL` (id 6, seconds until the server-side
  session expires).

  Reference: `official_libs/aerospike-client-go/admin_command.go` and
  `official_libs/aerospike-client-go/login_command.go`. Constants here use
  the same numeric values so wire captures from the Go client round-trip
  through this module unchanged.
  """

  alias Aerospike.Protocol.ResultCode

  # Proto type reserved for admin-protocol messages.
  @type_admin 2
  @proto_version 2

  # Command ids used by login and authenticate paths.
  @cmd_authenticate 0
  @cmd_login 20

  # Field ids used by login and authenticate paths.
  @field_user 0
  @field_credential 3
  @field_clear_password 4
  @field_session_token 5
  @field_session_ttl 6

  # Size of the admin header that sits between the 8-byte proto header and
  # the first field. Bytes 0..1 are reserved (always zero), byte 2 is the
  # command id, byte 3 is the field count, bytes 4..15 are zero padding.
  @admin_header_size 16

  # Total size of the two-part reply header the transport has to read up
  # front (8-byte proto + 16-byte admin header).
  @reply_header_size 24

  # Static bcrypt salt the server requires for internal-auth password
  # hashing. Matches Go `admin_command.go:hashPassword`.
  @bcrypt_salt "$2a$10$7EqJtq98hPqEX7fNZaFWoO"

  @typedoc """
  Parsed login reply.

    * `:ok_no_token` — server accepted the login but did not return a
      session token. Observed when security is enabled but the server
      treats the caller as anonymous (e.g. PKI with no user mapping).
    * `{:session, token, ttl_seconds_or_nil}` — server accepted the login
      and returned a session token. `ttl_seconds` is the value the server
      reported verbatim; callers typically subtract ~60 s to ensure the
      client expires the token before the server does.
    * `:security_not_enabled` — server has security disabled (result code
      52). Callers treat this as a successful, token-less authentication:
      the socket is usable and later commands will not need auth.
  """
  @type login_reply ::
          :ok_no_token
          | {:session, binary(), non_neg_integer() | nil}
          | :security_not_enabled

  @doc """
  Returns the proto type id reserved for admin-protocol messages. Exposed so
  the transport can validate reply headers without importing the constant.
  """
  @spec type_admin() :: 2
  def type_admin, do: @type_admin

  @doc """
  Returns the number of bytes a caller must read to see the full reply
  header (8-byte proto + 16-byte admin header). Kept as a module constant so
  the transport does not recompute it.
  """
  @spec reply_header_size() :: 24
  def reply_header_size, do: @reply_header_size

  @doc """
  Hashes `password` using the static bcrypt salt the Aerospike server expects
  for internal-auth credentials. Returns the 60-byte `$2a$10$...` hash as a
  binary ready to be placed verbatim into the `_CREDENTIAL` field.
  """
  @spec hash_password(binary()) :: binary()
  def hash_password(password) when is_binary(password) do
    Bcrypt.Base.hash_password(password, @bcrypt_salt)
  end

  @doc """
  Encodes an internal-auth login request. `hashed_password` must already be
  the bcrypt hash produced by `hash_password/1`; the server rejects a raw
  password here.
  """
  @spec encode_login_internal(binary(), binary()) :: iodata()
  def encode_login_internal(user, hashed_password)
      when is_binary(user) and is_binary(hashed_password) do
    fields = [
      encode_field(@field_user, user),
      encode_field(@field_credential, hashed_password)
    ]

    encode_command(@cmd_login, fields)
  end

  @doc """
  Encodes an external-auth login request. Callers must ensure the underlying
  transport is TLS-protected — the clear password rides on the wire verbatim.
  """
  @spec encode_login_external(binary(), binary(), binary()) :: iodata()
  def encode_login_external(user, hashed_password, clear_password)
      when is_binary(user) and is_binary(hashed_password) and is_binary(clear_password) do
    fields = [
      encode_field(@field_user, user),
      encode_field(@field_credential, hashed_password),
      encode_field(@field_clear_password, clear_password)
    ]

    encode_command(@cmd_login, fields)
  end

  @doc """
  Encodes a session-token-based authenticate request. Used by pool workers
  after the first full login has produced a token: every subsequent socket
  authenticates with the token instead of paying the bcrypt round trip.
  """
  @spec encode_authenticate(binary(), binary()) :: iodata()
  def encode_authenticate(user, session_token)
      when is_binary(user) and is_binary(session_token) do
    fields = [
      encode_field(@field_user, user),
      encode_field(@field_session_token, session_token)
    ]

    encode_command(@cmd_authenticate, fields)
  end

  @doc """
  Decodes the 24-byte reply header. Returns the result code atom (known
  codes) or raw integer (unknown), the `fieldCount`, and the remaining
  `body_length` to read before parsing fields.

  Callers handle the result code themselves so a non-zero "auth failed"
  reply still exposes its field-count and body length — today every error
  path skips the body (the server sends no fields on a non-zero reply), but
  surfacing the values here keeps the decoder honest against future server
  versions that add diagnostic fields to error replies.
  """
  @spec decode_reply_header(binary()) ::
          {:ok, result_code :: atom() | integer(), non_neg_integer(), non_neg_integer()}
          | {:error, :incomplete_header | {:wrong_version, integer()} | {:wrong_type, integer()}}
  def decode_reply_header(
        <<@proto_version, @type_admin, length::48-big, _admin_byte0::8, result_code::8,
          _admin_byte2::8, field_count::8, _tail::binary>>
      ) do
    # `length` covers the 16-byte admin header plus fields. The admin header
    # accounts for the first 16 bytes; the remaining `length - 16` bytes
    # are field data the caller must read next.
    #
    # The admin header layout inside the 16-byte block is:
    #
    #   byte 0: reserved (zero)
    #   byte 1: result code (see Aerospike.Protocol.ResultCode)
    #   byte 2: reserved (zero)
    #   byte 3: field count
    #   bytes 4..15: reserved padding (zero)
    #
    # Matches Go `admin_command.go` `_RESULT_CODE = 9` (buffer offset 9 =
    # admin offset 1) and `fieldCount = buffer[11]` (admin offset 3).
    remaining =
      case length - @admin_header_size do
        n when n >= 0 -> n
        _ -> 0
      end

    {:ok, result_code_atom(result_code), field_count, remaining}
  end

  def decode_reply_header(<<version, _type, _rest::binary>>)
      when version != @proto_version do
    {:error, {:wrong_version, version}}
  end

  def decode_reply_header(<<@proto_version, type, _rest::binary>>)
      when type != @type_admin do
    {:error, {:wrong_type, type}}
  end

  def decode_reply_header(header) when is_binary(header) and byte_size(header) < 24,
    do: {:error, :incomplete_header}

  def decode_reply_header(_), do: {:error, :incomplete_header}

  @doc """
  Decodes the field block of a login reply. `body` is exactly the
  `body_length` bytes returned by `decode_reply_header/1` (field data only,
  no admin header prefix). `field_count` comes from the reply header and
  bounds how many fields the decoder consumes.

  Returns `{:session, token, ttl}` when the server delivered a session
  token, or `:ok_no_token` when the reply carried no token (PKI and similar
  flows). Malformed or truncated field data surfaces as `:parse_error` so
  the caller can map it to a typed transport error.
  """
  @spec decode_login_fields(binary(), non_neg_integer()) ::
          {:ok, :ok_no_token | {:session, binary(), non_neg_integer() | nil}}
          | {:error, :parse_error}
  def decode_login_fields(body, field_count)
      when is_binary(body) and is_integer(field_count) and field_count >= 0 do
    parse_fields(body, field_count, %{})
  end

  ## Private helpers

  # Top-level command frame: 8-byte proto header + 16-byte admin header +
  # fields. `encode_command/2` is the only place the outer length is
  # written; every request path funnels through here so the length
  # accounting lives in one function.
  defp encode_command(command_id, fields) do
    fields_iodata = fields
    fields_size = IO.iodata_length(fields_iodata)
    admin_body_size = @admin_header_size + fields_size

    proto_header =
      <<@proto_version::8, @type_admin::8, admin_body_size::48-big>>

    field_count = length(fields)

    admin_header =
      <<0::8, 0::8, command_id::8, field_count::8, 0::96>>

    [proto_header, admin_header, fields_iodata]
  end

  defp encode_field(id, value) when is_binary(value) do
    size = byte_size(value) + 1
    [<<size::32-big, id::8>>, value]
  end

  defp result_code_atom(0), do: :ok

  defp result_code_atom(code) do
    case ResultCode.from_integer(code) do
      {:ok, atom} -> atom
      {:error, ^code} -> code
    end
  end

  defp parse_fields(_body, 0, acc), do: {:ok, build_login_reply(acc)}

  defp parse_fields(<<size::32-big, id::8, rest::binary>>, remaining, acc)
       when remaining > 0 and size >= 1 do
    value_len = size - 1

    case rest do
      <<value::binary-size(value_len), tail::binary>> ->
        parse_fields(tail, remaining - 1, store_field(acc, id, value))

      _ ->
        {:error, :parse_error}
    end
  end

  defp parse_fields(_other, _remaining, _acc), do: {:error, :parse_error}

  defp store_field(acc, @field_session_token, value), do: Map.put(acc, :token, value)

  defp store_field(acc, @field_session_ttl, <<ttl::32-big>>),
    do: Map.put(acc, :ttl, ttl)

  defp store_field(acc, _id, _value), do: acc

  defp build_login_reply(%{token: token} = acc) when is_binary(token) do
    {:session, token, Map.get(acc, :ttl)}
  end

  defp build_login_reply(_acc), do: :ok_no_token
end
