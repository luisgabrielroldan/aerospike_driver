defmodule Aerospike.Bench.Support.Fixtures do
  @moduledoc false

  alias Aerospike.Key
  alias Aerospike.Protocol.AsmMsg
  alias Aerospike.Protocol.AsmMsg.Operation
  alias Aerospike.Protocol.AsmMsg.Value

  @namespace "bench"
  @set "micro"
  @default_ttl 86_400
  @decode_profiles [
    {:small, 2, 128},
    {:medium, 8, 512},
    {:large, 16, 2048}
  ]

  def response_decode_inputs do
    Enum.into(@decode_profiles, %{}, fn {profile, bin_count, payload_bytes} ->
      key = Key.new(@namespace, @set, "mb007:#{profile}")
      bins = build_bins(profile, bin_count, payload_bytes)
      operations = Value.encode_bin_operations(bins, Operation.op_write())

      msg = %AsmMsg{
        result_code: 0,
        generation: 1,
        expiration: @default_ttl,
        operations: operations
      }

      label = "#{profile}/#{bin_count}bins/#{payload_bytes}B"
      {label, %{profile: profile, body: AsmMsg.encode(msg), key: key}}
    end)
  end

  defp build_bins(profile, bin_count, payload_bytes) do
    Enum.into(1..bin_count, %{}, fn index ->
      {"bin_#{index}", payload_for(profile, index, payload_bytes)}
    end)
  end

  defp payload_for(profile, index, payload_bytes) do
    seed = "#{profile}:#{index}:"
    repeats = div(payload_bytes + byte_size(seed) - 1, byte_size(seed))
    payload = :binary.copy(seed, repeats)
    binary_part(payload, 0, payload_bytes)
  end
end
