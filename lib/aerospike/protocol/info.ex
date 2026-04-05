defmodule Aerospike.Protocol.Info do
  @moduledoc false

  alias Aerospike.Protocol.Message

  @doc """
  Encodes an info request from a list of command strings.

  Commands are joined with newlines and a trailing newline is appended.
  The result is wrapped in a Message with type 1 (info).

  ## Examples

      iex> Aerospike.Protocol.Info.encode_request(["namespaces"])
      <<2, 1, 0, 0, 0, 0, 0, 11, "namespaces\\n">>

      iex> Aerospike.Protocol.Info.encode_request(["node", "build"])
      <<2, 1, 0, 0, 0, 0, 0, 11, "node\\nbuild\\n">>

      iex> Aerospike.Protocol.Info.encode_request([])
      <<2, 1, 0, 0, 0, 0, 0, 0>>

  """
  @spec encode_request([String.t()]) :: binary()
  def encode_request([]), do: Message.encode_info("")

  def encode_request(commands) when is_list(commands) do
    payload = Enum.join(commands, "\n") <> "\n"
    Message.encode_info(payload)
  end

  @doc """
  Decodes an info response body into a map of key-value pairs.

  The response format is newline-separated entries where each entry is either:
  - `key\\tvalue` (tab-separated key and value)
  - `key` (key only, value is empty string)

  ## Examples

      iex> Aerospike.Protocol.Info.decode_response("namespaces\\ttest;memory\\n")
      {:ok, %{"namespaces" => "test;memory"}}

      iex> Aerospike.Protocol.Info.decode_response("node\\tBB9050011AC4202\\nbuild\\t7.0.0.0\\n")
      {:ok, %{"node" => "BB9050011AC4202", "build" => "7.0.0.0"}}

      iex> Aerospike.Protocol.Info.decode_response("status\\n")
      {:ok, %{"status" => ""}}

      iex> Aerospike.Protocol.Info.decode_response("")
      {:ok, %{}}

  """
  @spec decode_response(binary()) :: {:ok, map()}
  def decode_response(body) when is_binary(body) do
    result =
      body
      |> String.trim_trailing("\n")
      |> case do
        "" -> %{}
        trimmed -> parse_entries(trimmed)
      end

    {:ok, result}
  end

  defp parse_entries(data) do
    data
    |> String.split("\n")
    |> Enum.reduce(%{}, fn entry, acc ->
      case String.split(entry, "\t", parts: 2) do
        [key, value] -> Map.put(acc, key, value)
        [key] -> Map.put(acc, key, "")
      end
    end)
  end

  @doc """
  Decodes a complete info response message (header + body).

  First decodes the message envelope, validates it's an info message (type 1),
  then parses the body.

  ## Examples

      iex> msg = <<2, 1, 0, 0, 0, 0, 0, 16, "namespaces\\ttest\\n">>
      iex> Aerospike.Protocol.Info.decode_message(msg)
      {:ok, %{"namespaces" => "test"}}

  """
  @spec decode_message(binary()) ::
          {:ok, map()} | {:error, :incomplete_header | :incomplete_body | :invalid_message_type}
  def decode_message(data) do
    with {:ok, {_version, type, body}} <- Message.decode(data) do
      if type == Message.type_info() do
        decode_response(body)
      else
        {:error, :invalid_message_type}
      end
    end
  end
end
