defmodule Aerospike.Bench.Scenarios.Default do
  @moduledoc false

  @scenarios %{
    quick: %{
      profile: :quick,
      duration_s: 3,
      warmup_s: 1,
      concurrency: [1],
      fanout_batch_sizes: [100],
      fanout_large_batch_sizes: [1000],
      payload_sizes: [256, 4096],
      payload_profiles: %{
        small: %{bin: "payload", bytes: 256},
        large: %{bin: "payload", bytes: 4096}
      }
    },
    default: %{
      profile: :default,
      duration_s: 10,
      warmup_s: 3,
      concurrency: [1, 4],
      fanout_batch_sizes: [100],
      fanout_large_batch_sizes: [1000],
      payload_sizes: [256, 4096],
      payload_profiles: %{
        small: %{bin: "payload", bytes: 256},
        large: %{bin: "payload", bytes: 4096}
      }
    },
    full: %{
      profile: :full,
      duration_s: 20,
      warmup_s: 5,
      concurrency: [1, 4, 16],
      fanout_batch_sizes: [100],
      fanout_large_batch_sizes: [1000],
      payload_sizes: [256, 4096, 16384],
      payload_profiles: %{
        small: %{bin: "payload", bytes: 256},
        large: %{bin: "payload", bytes: 16_384}
      }
    }
  }

  def all, do: @scenarios

  def fetch(profile) when is_atom(profile), do: Map.fetch(@scenarios, profile)

  def fetch(profile) when is_binary(profile) do
    profile
    |> String.trim()
    |> String.downcase()
    |> String.to_existing_atom()
    |> fetch()
  rescue
    ArgumentError -> :error
  end

  def fetch(_profile), do: :error
end
