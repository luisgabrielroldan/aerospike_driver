defmodule Aerospike.ConnectionTest do
  use ExUnit.Case, async: false

  @moduletag :integration

  alias Aerospike.Connection
  alias Aerospike.Protocol.Info

  describe "live Aerospike" do
    setup do
      host = System.get_env("AEROSPIKE_HOST", "127.0.0.1")
      port = String.to_integer(System.get_env("AEROSPIKE_PORT", "3000"))

      case await_aerospike(host, port) do
        :ok ->
          {:ok, host: host, port: port}

        {:error, reason} ->
          raise ExUnit.AssertionError,
            message: """
            Aerospike not reachable at #{host}:#{port} (#{inspect(reason)}).
            Start the server from the repo root: `docker compose up -d`
            """
      end
    end

    @tag timeout: 60_000
    test "info namespaces and build", %{host: host, port: port} do
      {:ok, conn} = Connection.connect(host: host, port: port, timeout: 10_000)

      on_exit(fn -> Connection.close(conn) end)

      {:ok, conn2, %{"namespaces" => ns}} =
        Connection.request_info(conn, ["namespaces"])

      assert is_binary(ns)
      assert String.length(ns) > 0

      assert {:ok, _conn3, %{"build" => build}} =
               Connection.request_info(conn2, ["build"])

      assert is_binary(build)
      assert String.length(build) > 0
    end

    test "login skips without credentials", %{host: host, port: port} do
      {:ok, conn} = Connection.connect(host: host, port: port, timeout: 10_000)
      on_exit(fn -> Connection.close(conn) end)

      assert {:ok, ^conn} = Connection.login(conn, [])
    end

    test "login with credentials completes (security off returns ok)", %{host: host, port: port} do
      {:ok, conn} = Connection.connect(host: host, port: port, timeout: 10_000)
      on_exit(fn -> Connection.close(conn) end)

      # Dummy credential blob; without bcrypt matching server policy, server may still
      # respond with SECURITY_NOT_* when security is disabled (typical local Docker).
      assert {:ok, _} =
               Connection.login(conn, user: "any", credential: <<0, 1, 2, 3, 4, 5>>)
    end

    test "close is idempotent", %{host: host, port: port} do
      {:ok, conn} = Connection.connect(host: host, port: port, timeout: 10_000)
      assert :ok = Connection.close(conn)
      assert :ok = Connection.close(conn)
    end

    test "request on closed connection returns error", %{host: host, port: port} do
      {:ok, conn} = Connection.connect(host: host, port: port, timeout: 10_000)
      :ok = Connection.close(conn)

      data = Info.encode_request(["namespaces"])
      assert {:error, :closed} = Connection.request(conn, data)
    end
  end

  describe "tcp without live Aerospike" do
    test "connect fails to unreachable port" do
      host = System.get_env("AEROSPIKE_HOST", "127.0.0.1")
      assert {:error, _} = Connection.connect(host: host, port: 1, timeout: 500)
    end
  end

  defp await_aerospike(host, port, attempts \\ 30)

  defp await_aerospike(host, port, attempts) do
    case :gen_tcp.connect(to_charlist(host), port, [:binary, {:active, false}], 2_000) do
      {:ok, s} ->
        :gen_tcp.close(s)
        :ok

      {:error, _reason} when attempts > 1 ->
        Process.sleep(1_000)
        await_aerospike(host, port, attempts - 1)

      {:error, reason} ->
        {:error, reason}
    end
  end
end
