defmodule Aerospike.Protocol.LoginTest do
  use ExUnit.Case, async: true

  alias Aerospike.Protocol.Login

  describe "hash_password/1" do
    test "produces the static-salt bcrypt hash the server expects" do
      hash = Login.hash_password("admin")

      # Static salt prefix from Aerospike's `login_command.go`.
      assert String.starts_with?(hash, "$2a$10$7EqJtq98hPqEX7fNZaFWoO")
      # Standard bcrypt digest length (header + salt + hash).
      assert byte_size(hash) == 60
    end

    test "is deterministic for the same password (same salt)" do
      assert Login.hash_password("hunter2") == Login.hash_password("hunter2")
    end

    test "distinct passwords produce distinct hashes" do
      refute Login.hash_password("left") == Login.hash_password("right")
    end
  end

  describe "encode_login_internal/2" do
    test "builds a 24-byte header followed by USER + CREDENTIAL fields" do
      hash = Login.hash_password("admin")
      frame = Login.encode_login_internal("admin", hash) |> IO.iodata_to_binary()

      # Proto header: version 2, type 2 (admin), 6-byte big-endian length.
      <<2, 2, length::48-big, admin_header::binary-size(16), rest::binary>> = frame

      # Admin header = command_id at byte 2, field_count at byte 3.
      <<0, 0, 20, 2, _padding::binary-size(12)>> = admin_header

      # Length counts the 16-byte admin header plus the fields.
      assert length == 16 + byte_size(rest)

      # Fields: USER (id 0, value "admin") + CREDENTIAL (id 3, value hash).
      <<size_user::32-big, 0::8, user_bytes::binary-size(size_user - 1), tail::binary>> = rest
      assert user_bytes == "admin"

      <<size_cred::32-big, 3::8, cred_bytes::binary-size(size_cred - 1)>> = tail
      assert cred_bytes == hash
    end
  end

  describe "encode_authenticate/2" do
    test "builds a 24-byte header followed by USER + SESSION_TOKEN fields" do
      token = <<1, 2, 3, 4, 5>>
      frame = Login.encode_authenticate("admin", token) |> IO.iodata_to_binary()

      <<2, 2, length::48-big, admin_header::binary-size(16), rest::binary>> = frame

      # command_id 0 = AUTHENTICATE, field_count 2.
      <<0, 0, 0, 2, _padding::binary-size(12)>> = admin_header
      assert length == 16 + byte_size(rest)

      <<size_user::32-big, 0::8, user_bytes::binary-size(size_user - 1), tail::binary>> = rest
      assert user_bytes == "admin"

      <<size_token::32-big, 5::8, token_bytes::binary-size(size_token - 1)>> = tail
      assert token_bytes == token
    end
  end

  describe "decode_reply_header/1" do
    test "extracts result code, field count, and body length" do
      # Body length is the byte-count *after* the 16-byte admin header.
      # The `length` field in the proto header covers admin header + fields,
      # so body_length = length - 16.
      body = <<0, 0, 0, 5, 0>>
      total_admin = 16 + byte_size(body)
      header = <<2, 2, total_admin::48-big, 0, 0, 0, 2, 0::96, body::binary>>

      assert {:ok, :ok, 2, 5} = Login.decode_reply_header(header)
    end

    test "surfaces known result code atoms" do
      header = <<2, 2, 16::48-big, 0, 52, 0, 0, 0::96>>
      assert {:ok, :security_not_enabled, 0, 0} = Login.decode_reply_header(header)
    end

    test "unknown result codes are returned as integers" do
      # Pick a code that ResultCode.from_integer/1 rejects (240 is unmapped).
      header = <<2, 2, 16::48-big, 0, 240, 0, 0, 0::96>>
      assert {:ok, 240, 0, 0} = Login.decode_reply_header(header)
    end

    test "wrong proto version is flagged" do
      header = <<1, 2, 16::48-big, 0::128>>
      assert {:error, {:wrong_version, 1}} = Login.decode_reply_header(header)
    end

    test "wrong proto type is flagged" do
      header = <<2, 3, 16::48-big, 0::128>>
      assert {:error, {:wrong_type, 3}} = Login.decode_reply_header(header)
    end

    test "short headers are rejected" do
      assert {:error, :incomplete_header} = Login.decode_reply_header(<<2, 2, 0, 0>>)
    end
  end

  describe "decode_login_fields/2" do
    test "pulls SESSION_TOKEN and SESSION_TTL out of a typical success reply" do
      token = :crypto.strong_rand_bytes(42)

      body =
        encode_field(5, token) <>
          encode_field(6, <<86_400::32-big>>)

      assert {:ok, {:session, ^token, 86_400}} = Login.decode_login_fields(body, 2)
    end

    test "token without TTL is returned with nil TTL" do
      token = "token-only"
      body = encode_field(5, token)

      assert {:ok, {:session, ^token, nil}} = Login.decode_login_fields(body, 1)
    end

    test "zero-field body is :ok_no_token" do
      assert {:ok, :ok_no_token} = Login.decode_login_fields(<<>>, 0)
    end

    test "unknown fields are skipped" do
      body = encode_field(99, <<"ignored">>)

      assert {:ok, :ok_no_token} = Login.decode_login_fields(body, 1)
    end

    test "truncated field body surfaces :parse_error" do
      # Claim a 10-byte value but only supply 3 bytes.
      body = <<11::32-big, 5::8, 1, 2, 3>>

      assert {:error, :parse_error} = Login.decode_login_fields(body, 1)
    end
  end

  defp encode_field(id, value) do
    size = byte_size(value) + 1
    <<size::32-big, id::8, value::binary>>
  end
end
