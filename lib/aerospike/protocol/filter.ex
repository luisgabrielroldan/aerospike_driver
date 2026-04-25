defmodule Aerospike.Protocol.Filter do
  @moduledoc false

  alias Aerospike.Filter
  alias Aerospike.Protocol.MessagePack

  @particle_integer 1
  @particle_string 3
  @particle_geojson 23

  @spec encode(Filter.t()) :: binary()
  def encode(%Filter{} = filter) do
    bin = if named_index?(filter), do: "", else: filter.bin_name
    particle = particle_wire(filter)
    begin_raw = encode_endpoint(filter, filter.begin)
    end_raw = encode_endpoint(filter, filter.end)

    <<
      1::8,
      byte_size(bin)::8,
      bin::binary,
      particle::8,
      byte_size(begin_raw)::32-big,
      begin_raw::binary,
      byte_size(end_raw)::32-big,
      end_raw::binary
    >>
  end

  @spec encode_ctx([Aerospike.Ctx.step()]) :: binary()
  def encode_ctx(ctx) when is_list(ctx) do
    ctx
    |> Enum.flat_map(fn {id, value} -> [id, encode_ctx_value(value)] end)
    |> MessagePack.pack!()
  end

  defp particle_wire(%Filter{index_type: t}) when t in [:geo_within, :geo_contains] do
    @particle_geojson
  end

  defp particle_wire(%Filter{index_type: t}) when t in [:list, :mapkeys, :mapvalues] do
    @particle_integer
  end

  defp particle_wire(%Filter{particle_type: :integer}), do: @particle_integer
  defp particle_wire(%Filter{particle_type: :string}), do: @particle_string

  defp named_index?(%Filter{index_name: name}) when is_binary(name) and name != "", do: true
  defp named_index?(_), do: false

  defp encode_endpoint(%Filter{particle_type: :integer}, value) when is_integer(value) do
    <<value::64-signed-big>>
  end

  defp encode_endpoint(%Filter{particle_type: :string}, value) when is_binary(value), do: value

  defp encode_ctx_value(value) when is_binary(value), do: {:particle_string, value}
  defp encode_ctx_value({:bytes, _} = value), do: value
  defp encode_ctx_value(value), do: value
end
