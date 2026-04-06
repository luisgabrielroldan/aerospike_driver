defmodule Aerospike.CursorTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Aerospike.Cursor
  alias Aerospike.Error

  test "encode/1 and decode/1 roundtrip" do
    cursor = %Cursor{
      partitions: [
        %{id: 1, digest: nil, bval: nil},
        %{id: 2, digest: <<0::160>>, bval: 0}
      ]
    }

    encoded = Cursor.encode(cursor)
    assert is_binary(encoded)
    assert {:ok, decoded} = Cursor.decode(encoded)
    assert decoded == cursor
  end

  test "encode/1 and decode/1 roundtrip with mixed partition entries" do
    d = :crypto.hash(:ripemd160, "k")

    cursor = %Cursor{
      partitions: [
        %{id: 0, digest: nil, bval: nil},
        %{id: 100, digest: d, bval: nil},
        %{id: 200, digest: nil, bval: -1},
        %{id: 300, digest: d, bval: 9_223_372_036_854_775_807}
      ]
    }

    assert {:ok, ^cursor} = cursor |> Cursor.encode() |> Cursor.decode()
  end

  test "decode/1 rejects invalid base64" do
    assert {:error, %Error{code: :parse_error}} = Cursor.decode("@@@not-base64@@@")
  end

  test "decode/1 rejects unknown binary payload (not v1 cursor)" do
    blob = <<0xFF, 0, 0>>
    encoded = Base.url_encode64(blob, padding: false)
    assert {:error, %Error{code: :parse_error}} = Cursor.decode(encoded)
  end

  test "encode/1 rejects wrong partition shape" do
    bad = %Cursor{partitions: [%{id: "x"}]}

    assert_raise ArgumentError, fn ->
      Cursor.encode(bad)
    end
  end

  test "encode/1 rejects extra map keys in partition entry" do
    bad = %Cursor{partitions: [%{id: 0, extra: 1}]}

    assert_raise ArgumentError, fn ->
      Cursor.encode(bad)
    end
  end

  test "decode/1 rejects truncated binary" do
    # version 1, count 1, but only 2 bytes of first entry (need 31)
    truncated = <<1, 0, 1, 1, 2>>
    encoded = Base.url_encode64(truncated, padding: false)
    assert {:error, %Error{code: :parse_error}} = Cursor.decode(encoded)
  end

  test "decode/1 rejects unknown version byte" do
    # version 2, count 0 — version must be 1
    bin = <<2, 0, 0>>
    encoded = Base.url_encode64(bin, padding: false)

    assert {:error, %Error{code: :parse_error, message: "invalid cursor version"}} =
             Cursor.decode(encoded)
  end

  test "decode/1 rejects count mismatch (header count exceeds payload)" do
    # v1, count 2, only one full entry (31 bytes) after header
    entry = <<0::16-big, 0::8, 0::160, 0::64-big-signed>>

    bin = <<1, 0, 2>> <> entry
    encoded = Base.url_encode64(bin, padding: false)
    assert {:error, %Error{code: :parse_error}} = Cursor.decode(encoded)
  end

  @tag :property
  property "encode/decode roundtrip for any valid cursor" do
    check all(
            len <- integer(0..64),
            parts <-
              list_of(
                fixed_map(%{
                  id: integer(0..4095),
                  digest:
                    one_of([
                      constant(nil),
                      binary(length: 20)
                    ]),
                  bval:
                    one_of([
                      constant(nil),
                      integer(-9_223_372_036_854_775_808..9_223_372_036_854_775_807)
                    ])
                }),
                length: len
              ),
            max_runs: 100
          ) do
      cursor = %Cursor{partitions: parts}
      assert {:ok, cursor} == cursor |> Cursor.encode() |> Cursor.decode()
    end
  end
end
