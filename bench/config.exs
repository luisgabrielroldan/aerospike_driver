defmodule Aerospike.Bench.Config do
  @moduledoc false

  alias Aerospike.Bench.Scenarios.Default

  @profile_env "BENCH_PROFILE"
  @duration_env "BENCH_DURATION_S"
  @warmup_env "BENCH_WARMUP_S"
  @concurrency_env "BENCH_CONCURRENCY"
  @payload_sizes_env "BENCH_PAYLOAD_SIZES"

  def load(overrides \\ %{}) when is_map(overrides) do
    profile = load_profile(overrides)

    scenario =
      case Default.fetch(profile) do
        {:ok, loaded} -> loaded
        :error -> raise(ArgumentError, "unknown benchmark profile: #{inspect(profile)}")
      end

    scenario
    |> apply_duration_override(overrides)
    |> apply_warmup_override(overrides)
    |> apply_concurrency_override(overrides)
    |> apply_payload_sizes_override(overrides)
  end

  defp load_profile(overrides) do
    case Map.fetch(overrides, :profile) do
      {:ok, value} -> normalize_profile(value)
      :error -> normalize_profile(System.get_env(@profile_env, "default"))
    end
  end

  defp normalize_profile(profile) when is_atom(profile), do: profile

  defp normalize_profile(profile) when is_binary(profile) do
    profile
    |> String.trim()
    |> String.downcase()
    |> String.to_atom()
  end

  defp apply_duration_override(config, overrides) do
    with :error <- Map.fetch(overrides, :duration_s),
         nil <- System.get_env(@duration_env) do
      config
    else
      {:ok, value} -> Map.put(config, :duration_s, to_positive_integer!(value, :duration_s))
      value -> Map.put(config, :duration_s, to_positive_integer!(value, :duration_s))
    end
  end

  defp apply_warmup_override(config, overrides) do
    with :error <- Map.fetch(overrides, :warmup_s),
         nil <- System.get_env(@warmup_env) do
      config
    else
      {:ok, value} -> Map.put(config, :warmup_s, to_non_negative_integer!(value, :warmup_s))
      value -> Map.put(config, :warmup_s, to_non_negative_integer!(value, :warmup_s))
    end
  end

  defp apply_concurrency_override(config, overrides) do
    with :error <- Map.fetch(overrides, :concurrency),
         nil <- System.get_env(@concurrency_env) do
      config
    else
      {:ok, value} -> Map.put(config, :concurrency, to_integer_list!(value, :concurrency))
      value -> Map.put(config, :concurrency, to_integer_list!(value, :concurrency))
    end
  end

  defp apply_payload_sizes_override(config, overrides) do
    with :error <- Map.fetch(overrides, :payload_sizes),
         nil <- System.get_env(@payload_sizes_env) do
      config
    else
      {:ok, value} -> Map.put(config, :payload_sizes, to_integer_list!(value, :payload_sizes))
      value -> Map.put(config, :payload_sizes, to_integer_list!(value, :payload_sizes))
    end
  end

  defp to_positive_integer!(value, _field) when is_integer(value) and value > 0, do: value

  defp to_positive_integer!(value, field),
    do: value |> to_integer!(field) |> ensure_positive!(field)

  defp to_non_negative_integer!(value, _field) when is_integer(value) and value >= 0, do: value

  defp to_non_negative_integer!(value, field),
    do: value |> to_integer!(field) |> ensure_non_negative!(field)

  defp to_integer_list!(value, field) when is_list(value) do
    value
    |> Enum.map(&to_positive_integer!(&1, field))
    |> reject_empty!(field)
  end

  defp to_integer_list!(value, field) when is_binary(value) do
    value
    |> String.split(",", trim: true)
    |> Enum.map(&String.trim/1)
    |> Enum.map(&to_positive_integer!(&1, field))
    |> reject_empty!(field)
  end

  defp to_integer_list!(value, field), do: raise(ArgumentError, invalid_message(field, value))

  defp to_integer!(value, _field) when is_integer(value), do: value

  defp to_integer!(value, field) when is_binary(value) do
    case Integer.parse(String.trim(value)) do
      {parsed, ""} -> parsed
      _ -> raise(ArgumentError, invalid_message(field, value))
    end
  end

  defp to_integer!(value, field), do: raise(ArgumentError, invalid_message(field, value))

  defp ensure_positive!(value, _field) when value > 0, do: value
  defp ensure_positive!(value, field), do: raise(ArgumentError, invalid_message(field, value))

  defp ensure_non_negative!(value, _field) when value >= 0, do: value
  defp ensure_non_negative!(value, field), do: raise(ArgumentError, invalid_message(field, value))

  defp reject_empty!(items, _field) when length(items) > 0, do: items
  defp reject_empty!(value, field), do: raise(ArgumentError, invalid_message(field, value))

  defp invalid_message(field, value), do: "invalid #{field} value: #{inspect(value)}"
end
