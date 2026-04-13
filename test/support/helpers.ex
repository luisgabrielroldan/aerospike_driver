defmodule Aerospike.Test.Helpers do
  @moduledoc false

  alias Aerospike.Connection
  alias Aerospike.Error
  alias Aerospike.Key
  alias Aerospike.Protocol.AsmMsg
  alias Aerospike.Protocol.AsmMsg.Value
  alias Aerospike.Protocol.Message
  alias Aerospike.Protocol.Response

  @doc """
  Builds a unique string user key in the given namespace and set.
  """
  @spec unique_key(String.t(), String.t()) :: Key.t()
  def unique_key(namespace \\ "test", set \\ "test") do
    suffix = :crypto.strong_rand_bytes(8) |> Base.encode16(case: :lower)
    Key.new(namespace, set, "test:#{suffix}")
  end

  @doc """
  Best-effort delete for test cleanup. Opens a fresh connection because `on_exit`
  runs after the test process dies, so the original socket is already closed.
  """
  @spec cleanup_key(Key.t(), keyword()) :: :ok
  def cleanup_key(%Key{} = key, opts \\ []) do
    host = Keyword.get(opts, :host, "127.0.0.1")
    port = Keyword.get(opts, :port, 3000)

    case Connection.connect(host: host, port: port) do
      {:ok, conn} ->
        {:ok, conn} = Connection.login(conn)
        msg = AsmMsg.delete_command(key.namespace, key.set, key.digest)
        wire = IO.iodata_to_binary(Message.encode_as_msg_iodata(AsmMsg.encode(msg)))
        _ = Connection.request(conn, wire)
        Connection.close(conn)

      _ ->
        :ok
    end

    :ok
  end

  @doc """
  Best-effort secondary index drop for test cleanup. Opens a fresh connection
  because `on_exit` runs after the supervised client is torn down, so calling
  `Aerospike.drop_index/3` with the test's conn name would fail.
  """
  @spec cleanup_index(String.t(), String.t(), keyword()) :: :ok
  def cleanup_index(namespace, index_name, opts \\ [])
      when is_binary(namespace) and is_binary(index_name) do
    send_info(["sindex-delete:ns=#{namespace};indexname=#{index_name}"], opts)
  end

  @doc """
  Best-effort UDF removal for test cleanup. Same motivation as `cleanup_index/3`:
  `on_exit` cannot use the supervised client since it's already torn down.
  """
  @spec cleanup_udf(String.t(), keyword()) :: :ok
  def cleanup_udf(udf_name, opts \\ []) when is_binary(udf_name) do
    send_info(["udf-remove:filename=#{udf_name};"], opts)
  end

  defp send_info(commands, opts) do
    host = Keyword.get(opts, :host, "127.0.0.1")
    port = Keyword.get(opts, :port, 3000)

    case Connection.connect(host: host, port: port) do
      {:ok, conn} ->
        {:ok, conn} = Connection.login(conn)
        _ = Connection.request_info(conn, commands)
        Connection.close(conn)

      _ ->
        :ok
    end

    :ok
  end

  @doc """
  Sends a wire message and decodes the AS_MSG response.

  Returns `{:ok, conn, msg}` where `conn` has refreshed idle deadline and `msg` is the decoded AsmMsg.
  Returns `{:error, reason}` on network or protocol errors.
  """
  @spec send_command(Connection.t(), binary()) ::
          {:ok, Connection.t(), AsmMsg.t()} | {:error, Error.t()}
  def send_command(%Connection{} = conn, wire) when is_binary(wire) do
    case Connection.request(conn, wire) do
      {:ok, conn2, version, type, body} ->
        with :ok <- validate_as_msg(version, type),
             {:ok, msg} <- AsmMsg.decode(body) do
          {:ok, conn2, msg}
        else
          {:error, :invalid_message_frame} ->
            {:error,
             Error.from_result_code(:parse_error, message: "unexpected protocol message type")}

          {:error, reason} ->
            {:error, Error.from_result_code(:parse_error, message: inspect(reason))}
        end

      {:error, reason} ->
        {:error, Error.from_result_code(:network_error, message: inspect(reason))}
    end
  end

  defp validate_as_msg(version, type) do
    if version == Message.proto_version() and type == Message.type_as_msg() do
      :ok
    else
      {:error, :invalid_message_frame}
    end
  end

  @doc """
  PUT operation: sends put wire message and parses write response.
  Returns `{:ok, conn}` or `{:error, Error.t()}`.
  """
  @spec put(Connection.t(), Key.t(), map()) :: {:ok, Connection.t()} | {:error, Error.t()}
  def put(conn, key, bins) do
    ops = Value.encode_bin_operations(bins)
    msg = AsmMsg.write_command(key.namespace, key.set, key.digest, ops)
    wire = IO.iodata_to_binary(Message.encode_as_msg_iodata(AsmMsg.encode(msg)))

    case send_command(conn, wire) do
      {:ok, conn2, msg} ->
        case Response.parse_write_response(msg) do
          :ok -> {:ok, conn2}
          {:error, _} = err -> err
        end

      {:error, _} = err ->
        err
    end
  end

  @doc """
  GET operation: sends get wire message and parses record response.
  Returns `{:ok, conn, record}` or `{:error, Error.t()}`.
  """
  @spec get(Connection.t(), Key.t()) ::
          {:ok, Connection.t(), Aerospike.Record.t()} | {:error, Error.t()}
  def get(conn, key) do
    msg = AsmMsg.read_command(key.namespace, key.set, key.digest)
    wire = IO.iodata_to_binary(Message.encode_as_msg_iodata(AsmMsg.encode(msg)))

    case send_command(conn, wire) do
      {:ok, conn2, msg} ->
        case Response.parse_record_response(msg, key) do
          {:ok, record} -> {:ok, conn2, record}
          {:error, _} = err -> err
        end

      {:error, _} = err ->
        err
    end
  end

  @doc """
  DELETE operation: sends delete wire message and parses delete response.
  Returns `{:ok, conn, existed?}` or `{:error, Error.t()}`.
  """
  @spec delete(Connection.t(), Key.t()) ::
          {:ok, Connection.t(), boolean()} | {:error, Error.t()}
  def delete(conn, key) do
    msg = AsmMsg.delete_command(key.namespace, key.set, key.digest)
    wire = IO.iodata_to_binary(Message.encode_as_msg_iodata(AsmMsg.encode(msg)))

    case send_command(conn, wire) do
      {:ok, conn2, msg} ->
        case Response.parse_delete_response(msg) do
          {:ok, existed?} -> {:ok, conn2, existed?}
          {:error, _} = err -> err
        end

      {:error, _} = err ->
        err
    end
  end

  @doc """
  EXISTS operation: sends exists wire message and parses exists response.
  Returns `{:ok, conn, exists?}` or `{:error, Error.t()}`.
  """
  @spec exists(Connection.t(), Key.t()) ::
          {:ok, Connection.t(), boolean()} | {:error, Error.t()}
  def exists(conn, key) do
    msg = AsmMsg.exists_command(key.namespace, key.set, key.digest)
    wire = IO.iodata_to_binary(Message.encode_as_msg_iodata(AsmMsg.encode(msg)))

    case send_command(conn, wire) do
      {:ok, conn2, msg} ->
        case Response.parse_exists_response(msg) do
          {:ok, exists?} -> {:ok, conn2, exists?}
          {:error, _} = err -> err
        end

      {:error, _} = err ->
        err
    end
  end

  @doc """
  TOUCH operation: sends touch wire message and parses write response.
  Returns `{:ok, conn}` or `{:error, Error.t()}`.
  """
  @spec touch(Connection.t(), Key.t()) :: {:ok, Connection.t()} | {:error, Error.t()}
  def touch(conn, key) do
    msg = AsmMsg.touch_command(key.namespace, key.set, key.digest)
    wire = IO.iodata_to_binary(Message.encode_as_msg_iodata(AsmMsg.encode(msg)))

    case send_command(conn, wire) do
      {:ok, conn2, msg} ->
        case Response.parse_write_response(msg) do
          :ok -> {:ok, conn2}
          {:error, _} = err -> err
        end

      {:error, _} = err ->
        err
    end
  end

  @doc false
  @spec put_wire(Key.t(), map()) :: binary()
  def put_wire(%Key{} = key, bins) when is_map(bins) do
    ops = Value.encode_bin_operations(bins)
    msg = AsmMsg.write_command(key.namespace, key.set, key.digest, ops)
    IO.iodata_to_binary(Message.encode_as_msg_iodata(AsmMsg.encode(msg)))
  end

  @doc false
  @spec get_wire(Key.t()) :: binary()
  def get_wire(%Key{} = key) do
    msg = AsmMsg.read_command(key.namespace, key.set, key.digest)
    IO.iodata_to_binary(Message.encode_as_msg_iodata(AsmMsg.encode(msg)))
  end

  @doc false
  @spec delete_wire(Key.t()) :: binary()
  def delete_wire(%Key{} = key) do
    msg = AsmMsg.delete_command(key.namespace, key.set, key.digest)
    IO.iodata_to_binary(Message.encode_as_msg_iodata(AsmMsg.encode(msg)))
  end

  @doc false
  @spec exists_wire(Key.t()) :: binary()
  def exists_wire(%Key{} = key) do
    msg = AsmMsg.exists_command(key.namespace, key.set, key.digest)
    IO.iodata_to_binary(Message.encode_as_msg_iodata(AsmMsg.encode(msg)))
  end

  @doc false
  @spec touch_wire(Key.t()) :: binary()
  def touch_wire(%Key{} = key) do
    msg = AsmMsg.touch_command(key.namespace, key.set, key.digest)
    IO.iodata_to_binary(Message.encode_as_msg_iodata(AsmMsg.encode(msg)))
  end
end
