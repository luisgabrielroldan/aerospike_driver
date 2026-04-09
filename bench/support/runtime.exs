defmodule Aerospike.Bench.Support.Runtime do
  @moduledoc false

  @host_env "AEROSPIKE_HOST"
  @port_env "AEROSPIKE_PORT"
  @namespace_env "BENCH_NAMESPACE"
  @set_env "BENCH_SET"

  def connection_target(default_set) when is_binary(default_set) do
    %{
      host: System.get_env(@host_env, "127.0.0.1"),
      port: env_positive_integer(@port_env, 3000),
      namespace: System.get_env(@namespace_env, "test"),
      set: System.get_env(@set_env, default_set)
    }
  end

  def env_positive_integer(name, default)
      when is_binary(name) and is_integer(default) and default > 0 do
    case System.get_env(name) do
      nil ->
        default

      value ->
        case Integer.parse(String.trim(value)) do
          {parsed, ""} when parsed > 0 -> parsed
          _ -> raise ArgumentError, "invalid #{name} value: #{inspect(value)}"
        end
    end
  end

  def print_metadata(metadata, extra \\ %{}) when is_map(metadata) and is_map(extra) do
    IO.puts("Benchmark metadata:")
    IO.inspect(Map.merge(metadata, extra), pretty: true, limit: :infinity, charlists: :as_lists)
  end
end
