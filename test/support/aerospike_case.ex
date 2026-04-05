defmodule Aerospike.Test.AerospikeCase do
  @moduledoc """
  ExUnit case template for integration tests against a live Aerospike server.

  Note: The socket is owned by the test process. When the test process exits,
  BEAM automatically closes the socket. We do NOT use `on_exit` for Connection.close
  because `on_exit` runs in a separate process after the test dies, by which time
  the socket is already closed.
  """

  defmacro __using__(_opts) do
    quote do
      use ExUnit.Case, async: false

      @moduletag :integration

      setup do
        host = System.get_env("AEROSPIKE_HOST", "127.0.0.1")
        port = System.get_env("AEROSPIKE_PORT", "3000") |> String.to_integer()

        {:ok, conn} = Aerospike.Connection.connect(host: host, port: port)
        {:ok, conn} = Aerospike.Connection.login(conn)

        {:ok, conn: conn, host: host, port: port}
      end
    end
  end
end
