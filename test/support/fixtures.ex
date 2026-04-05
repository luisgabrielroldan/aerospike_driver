defmodule Aerospike.Test.Fixtures do
  @moduledoc """
  Golden-file fixtures for protocol testing.

  These fixtures represent known-good wire protocol bytes. They are used
  to verify that our encode/decode implementations produce byte-compatible
  results with the Aerospike wire protocol.
  """

  @doc """
  Info request for "namespaces" command.

  Protocol:
  - 8-byte header: version=2, type=1 (info), length=11
  - Body: "namespaces\\n"
  """
  def info_request_namespaces do
    # version=2, type=1, length=11 (byte count of "namespaces\n")
    header = <<2, 1, 0, 0, 0, 0, 0, 11>>
    body = "namespaces\n"
    header <> body
  end

  @doc """
  Info request for multiple commands: "node" and "build".

  Protocol:
  - 8-byte header: version=2, type=1 (info), length=11
  - Body: "node\\nbuild\\n"
  """
  def info_request_multi do
    header = <<2, 1, 0, 0, 0, 0, 0, 11>>
    body = "node\nbuild\n"
    header <> body
  end

  @doc """
  Info response with namespaces.

  Protocol:
  - 8-byte header: version=2, type=1 (info), length=20
  - Body: "namespaces\\ttest;bar\\n"
  """
  def info_response_namespaces do
    body = "namespaces\ttest;bar\n"
    header = <<2, 1, 0, 0, 0, 0, 0, byte_size(body)>>
    header <> body
  end

  @doc """
  Simple AS_MSG get command.

  This represents a GET command for:
  - Namespace: "test"
  - Set: "users"
  - Digest: 20 bytes of 0xAB

  AS_MSG header (22 bytes):
  - header_size: 22
  - info1: 0x03 (READ | GET_ALL)
  - info2-4, result_code: 0
  - generation: 0
  - expiration: 0
  - timeout: 0
  - field_count: 3
  - op_count: 0

  Fields:
  - namespace (type=0): "test"
  - set (type=1): "users"
  - digest (type=4): 20 bytes of 0xAB
  """
  def asm_msg_get_simple do
    import Bitwise

    # AS_MSG header (22 bytes)
    # READ | GET_ALL
    info1 = 0x01 ||| 0x02

    header =
      <<
        22,
        info1,
        0,
        0,
        0,
        0,
        # generation
        0::32-big,
        # expiration
        0::32-big,
        # timeout
        0::32-big,
        # field_count
        3::16-big,
        # op_count
        0::16-big
      >>

    # Field 1: namespace "test" (type=0)
    ns_field = <<5::32-big, 0, "test">>

    # Field 2: set "users" (type=1)
    set_field = <<6::32-big, 1, "users">>

    # Field 3: digest (type=4, 20 bytes of 0xAB)
    digest = :binary.copy(<<0xAB>>, 20)
    digest_field = <<21::32-big, 4, digest::binary>>

    payload = header <> ns_field <> set_field <> digest_field

    # Protocol header: version=2, type=3 (AS_MSG), length=payload size
    proto_header = <<2, 3, 0, 0, 0, 0, 0, byte_size(payload)>>

    proto_header <> payload
  end

  @doc """
  AS_MSG exists command (READ | NOBINDATA, no operations).

  Same key material as `asm_msg_get_simple`: namespace "test", set "users", digest 20x0xAB.
  """
  def asm_msg_exists_simple do
    import Bitwise

    info1 = 0x01 ||| 0x20

    header =
      <<
        22,
        info1,
        0,
        0,
        0,
        0,
        0::32-big,
        0::32-big,
        0::32-big,
        3::16-big,
        0::16-big
      >>

    digest = :binary.copy(<<0xAB>>, 20)
    ns_field = <<5::32-big, 0, "test">>
    set_field = <<6::32-big, 1, "users">>
    digest_field = <<21::32-big, 4, digest::binary>>

    payload = header <> ns_field <> set_field <> digest_field
    proto_header = <<2, 3, 0, 0, 0, 0, 0, byte_size(payload)>>

    proto_header <> payload
  end

  @doc """
  AS_MSG touch command (WRITE + single touch operation).

  Same key material as `asm_msg_put_simple` digest pattern (20x0xCD) for tests.
  """
  def asm_msg_touch_simple do
    header =
      <<
        22,
        0,
        0x01,
        0,
        0,
        0,
        0::32-big,
        0::32-big,
        0::32-big,
        3::16-big,
        1::16-big
      >>

    digest = :binary.copy(<<0xCD>>, 20)
    ns_field = <<5::32-big, 0, "test">>
    set_field = <<6::32-big, 1, "users">>
    digest_field = <<21::32-big, 4, digest::binary>>
    touch_op = <<4::32-big, 11, 0, 0, 0>>

    payload = header <> ns_field <> set_field <> digest_field <> touch_op
    proto_header = <<2, 3, 0, 0, 0, 0, 0, byte_size(payload)>>

    proto_header <> payload
  end

  @doc """
  Simple AS_MSG put command.

  This represents a PUT command for:
  - Namespace: "test"
  - Set: "users"
  - Digest: 20 bytes of 0xCD
  - Bin "name" = "Alice" (string)

  AS_MSG header (22 bytes):
  - header_size: 22
  - info1: 0
  - info2: 0x01 (WRITE)
  - info3-4, result_code: 0
  - generation: 0
  - expiration: 0
  - timeout: 0
  - field_count: 3
  - op_count: 1
  """
  def asm_msg_put_simple do
    # AS_MSG header (22 bytes)
    header =
      <<
        22,
        0,
        0x01,
        0,
        0,
        0,
        # generation
        0::32-big,
        # expiration
        0::32-big,
        # timeout
        0::32-big,
        # field_count
        3::16-big,
        # op_count
        1::16-big
      >>

    # Field 1: namespace "test" (type=0)
    ns_field = <<5::32-big, 0, "test">>

    # Field 2: set "users" (type=1)
    set_field = <<6::32-big, 1, "users">>

    # Field 3: digest (type=4, 20 bytes of 0xCD)
    digest = :binary.copy(<<0xCD>>, 20)
    digest_field = <<21::32-big, 4, digest::binary>>

    # Operation: write string "Alice" to bin "name"
    # size = 4 + name_len + data_len = 4 + 4 + 5 = 13
    # op_type=2 (WRITE), particle_type=3 (STRING), reserved=0, name_len=4
    operation = <<13::32-big, 2, 3, 0, 4, "name", "Alice">>

    payload = header <> ns_field <> set_field <> digest_field <> operation

    # Protocol header: version=2, type=3 (AS_MSG), length=payload size
    proto_header = <<2, 3, 0, 0, 0, 0, 0, byte_size(payload)>>

    proto_header <> payload
  end

  @doc """
  AS_MSG response with success and bins.

  This represents a successful response with:
  - result_code: 0 (OK)
  - generation: 5
  - expiration: 3600
  - Bin "name" = "Alice" (string)
  - Bin "count" = 42 (integer)
  """
  def asm_msg_response_ok do
    # AS_MSG header (22 bytes)
    # info3 bit 0 (LAST) should be set for final response
    header =
      <<
        22,
        0,
        0,
        0x01,
        0,
        0,
        # generation
        5::32-big,
        # expiration
        3600::32-big,
        # timeout
        0::32-big,
        # field_count
        0::16-big,
        # op_count
        2::16-big
      >>

    # Operation 1: bin "name" = "Alice" (read response includes bin value)
    # op_type=1 (READ), particle_type=3 (STRING)
    op1 = <<13::32-big, 1, 3, 0, 4, "name", "Alice">>

    # Operation 2: bin "count" = 42 (as 8-byte integer)
    # op_type=1 (READ), particle_type=1 (INTEGER)
    op2 = <<17::32-big, 1, 1, 0, 5, "count", 42::64-big-signed>>

    payload = header <> op1 <> op2

    # Protocol header
    proto_header = <<2, 3, 0, 0, 0, 0, 0, byte_size(payload)>>

    proto_header <> payload
  end

  @doc """
  AS_MSG response with KEY_NOT_FOUND error.
  """
  def asm_msg_response_not_found do
    # AS_MSG header with result_code=2 (KEY_NOT_FOUND)
    header =
      <<
        22,
        0,
        0,
        0x01,
        0,
        2,
        # generation
        0::32-big,
        # expiration
        0::32-big,
        # timeout
        0::32-big,
        # field_count
        0::16-big,
        # op_count
        0::16-big
      >>

    # Protocol header
    proto_header = <<2, 3, 0, 0, 0, 0, 0, byte_size(header)>>

    proto_header <> header
  end
end
