defmodule Aerospike.Protocol.AdminTest do
  use ExUnit.Case, async: true

  alias Aerospike.Protocol.Admin
  alias Aerospike.Protocol.Message

  describe "encode_login/2" do
    test "produces message type 2 (admin) and version 2 in header" do
      bin = Admin.encode_login("testuser", <<1, 2, 3, 4>>)
      assert byte_size(bin) > 24

      assert {:ok, {2, 2, body}} = Message.decode(bin)
      assert byte_size(body) >= 16
      assert binary_part(body, 2, 1) == <<20>>
      assert binary_part(body, 3, 1) == <<2>>
    end

    test "field layout matches admin field encoding" do
      bin = Admin.encode_login("ab", <<9, 9>>)
      {:ok, {_v, _t, body}} = Message.decode(bin)

      rest = binary_part(body, 16, byte_size(body) - 16)
      assert <<3::32-big, 0::8, "ab"::binary>> == binary_part(rest, 0, 7)
    end
  end

  describe "decode_admin_body/1" do
    test "reads result code at byte index 1" do
      body = :binary.copy(<<0>>, 16)
      {:ok, %{result_code: :ok, raw: 0}} = Admin.decode_admin_body(body)
    end

    test "maps non-zero wire code at byte index 1" do
      body = put_byte(:binary.copy(<<0>>, 16), 1, 2)
      {:ok, %{result_code: :key_not_found, raw: 2}} = Admin.decode_admin_body(body)
    end

    test "returns unknown_result_code for unmapped wire codes" do
      body = put_byte(:binary.copy(<<0>>, 16), 1, 254)
      {:ok, %{result_code: :unknown_result_code, raw: 254}} = Admin.decode_admin_body(body)
    end

    test "returns short_body error for bodies smaller than 2 bytes" do
      assert {:error, :short_body} = Admin.decode_admin_body(<<0>>)
      assert {:error, :short_body} = Admin.decode_admin_body(<<>>)
    end
  end

  describe "decode_response/1" do
    test "full frame" do
      body = put_byte(:binary.copy(<<0>>, 16), 1, 52)
      frame = Message.encode(2, 2, body)

      {:ok, %{result_code: :security_not_enabled, raw: 52}} =
        Admin.decode_response(frame)
    end
  end

  describe "decode_login_session_fields/1" do
    test "returns nils for body <= 16 bytes" do
      body = :binary.copy(<<0>>, 16)
      assert %{session_token: nil, session_ttl_sec: nil} = Admin.decode_login_session_fields(body)
    end

    test "parses session_token (field id 5)" do
      # 16-byte admin header + session_token field
      # Field format: 4-byte length (data_len + 1), 1-byte id, then data
      token = "my-session-token"
      token_field = <<byte_size(token) + 1::32-big, 5::8, token::binary>>
      body = :binary.copy(<<0>>, 16) <> token_field

      result = Admin.decode_login_session_fields(body)
      assert result.session_token == token
      assert result.session_ttl_sec == nil
    end

    test "parses session_ttl_sec (field id 6)" do
      # Field format: 4-byte length (4 + 1 = 5), 1-byte id (6), 4-byte uint32
      ttl = 3600
      ttl_field = <<5::32-big, 6::8, ttl::32-big>>
      body = :binary.copy(<<0>>, 16) <> ttl_field

      result = Admin.decode_login_session_fields(body)
      assert result.session_token == nil
      assert result.session_ttl_sec == ttl
    end

    test "parses both session_token and session_ttl_sec" do
      token = "tok"
      ttl = 7200
      token_field = <<byte_size(token) + 1::32-big, 5::8, token::binary>>
      ttl_field = <<5::32-big, 6::8, ttl::32-big>>
      body = :binary.copy(<<0>>, 16) <> token_field <> ttl_field

      result = Admin.decode_login_session_fields(body)
      assert result.session_token == token
      assert result.session_ttl_sec == ttl
    end

    test "ignores unknown field ids" do
      # Field with id 99 (unknown)
      unknown_field = <<4::32-big, 99::8, "abc"::binary>>
      body = :binary.copy(<<0>>, 16) <> unknown_field

      result = Admin.decode_login_session_fields(body)
      assert result.session_token == nil
      assert result.session_ttl_sec == nil
    end
  end

  describe "login_command_id/0" do
    test "returns the login command constant" do
      assert Admin.login_command_id() == 20
    end
  end

  describe "decode_login_session_fields edge cases" do
    test "handles truncated session field gracefully" do
      body = :binary.copy(<<0>>, 16) <> <<0, 0, 0, 10, 5>>
      result = Admin.decode_login_session_fields(body)
      assert result.session_token == nil
      assert result.session_ttl_sec == nil
    end

    test "handles session_ttl_sec field with non-4-byte data" do
      bad_ttl_field = <<3::32-big, 6::8, 0, 0>>
      body = :binary.copy(<<0>>, 16) <> bad_ttl_field
      result = Admin.decode_login_session_fields(body)
      assert result.session_ttl_sec == nil
    end

    test "handles body with no extra fields after admin header" do
      body = :binary.copy(<<0>>, 17)
      result = Admin.decode_login_session_fields(body)
      assert result.session_token == nil
      assert result.session_ttl_sec == nil
    end
  end

  defp put_byte(bin, index, byte) do
    before = binary_part(bin, 0, index)
    after_ = binary_part(bin, index + 1, byte_size(bin) - index - 1)
    before <> <<byte>> <> after_
  end
end
