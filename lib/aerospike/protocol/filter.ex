defmodule Aerospike.Protocol.Filter do
  @moduledoc false

  # INDEX_RANGE field payload (field header is added by `AsmMsg.Field.encode/1`).
  #
  # Wire layout (per Go/Java/C clients):
  #   1 byte:  filter count (always 1)
  #   1 byte:  bin name length
  #   N bytes: bin name (UTF-8)
  #   1 byte:  particle type (INTEGER=1, STRING=3, GEOJSON=23)
  #   4 bytes: begin value size (int32 BE)
  #   N bytes: begin value
  #   4 bytes: end value size (int32 BE)
  #   N bytes: end value
  #
  # Integer endpoints: 8-byte big-endian signed int64.
  # String/GeoJSON: raw UTF-8 bytes.

  alias Aerospike.Filter
  alias Aerospike.Protocol.MessagePack

  @particle_integer 1
  @particle_string 3
  @particle_geojson 23

  @spec encode(Filter.t()) :: binary()
  def encode(%Filter{} = f) do
    bin = if named_index?(f), do: "", else: f.bin_name
    name_len = byte_size(bin)

    particle = particle_wire(f)
    begin_raw = encode_endpoint(f, f.begin)
    end_raw = encode_endpoint(f, f.end)
    bsz = byte_size(begin_raw)
    esz = byte_size(end_raw)

    <<
      1::8,
      name_len::8,
      bin::binary,
      particle::8,
      bsz::32-big,
      begin_raw::binary,
      esz::32-big,
      end_raw::binary
    >>
  end

  defp particle_wire(%Filter{index_type: t}) when t in [:geo_within, :geo_contains] do
    @particle_geojson
  end

  defp particle_wire(%Filter{particle_type: :integer}), do: @particle_integer
  defp particle_wire(%Filter{particle_type: :string}), do: @particle_string

  defp named_index?(%Filter{index_name: name}) when is_binary(name) and name != "", do: true
  defp named_index?(_), do: false

  @spec encode_ctx([Aerospike.Ctx.step()]) :: binary()
  def encode_ctx(ctx) when is_list(ctx) do
    ctx
    |> Enum.flat_map(fn {id, value} -> [id, encode_ctx_value(value)] end)
    |> MessagePack.pack!()
  end

  defp encode_endpoint(%Filter{particle_type: :integer}, n) when is_integer(n) do
    <<n::64-signed-big>>
  end

  defp encode_endpoint(%Filter{particle_type: :string}, s) when is_binary(s) do
    s
  end

  defp encode_ctx_value(value) when is_binary(value), do: {:particle_string, value}
  defp encode_ctx_value({:bytes, _} = value), do: value
  defp encode_ctx_value(value), do: value
end
