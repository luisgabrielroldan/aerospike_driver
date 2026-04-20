defmodule Aerospike.Transport.TcpTest do
  use ExUnit.Case, async: true

  alias Aerospike.Error
  alias Aerospike.Transport.Tcp

  # Tier 1.5 Task 4 — exercise `command/3`'s read-deadline plumbing end to
  # end against a real loopback `:gen_tcp` listener. Integration GET tests
  # already cover the happy path against a live Aerospike; these tests
  # pin behaviour the happy-path can't observe (partial deadline blowout,
  # connection closed mid-read).

  setup do
    {:ok, listener} = :gen_tcp.listen(0, [:binary, active: false, packet: :raw, reuseaddr: true])
    {:ok, port} = :inet.port(listener)

    on_exit(fn ->
      _ = :gen_tcp.close(listener)
    end)

    %{listener: listener, port: port}
  end

  describe "command/3 read deadline" do
    test "times out with a :timeout error when the server never replies", %{
      listener: listener,
      port: port
    } do
      server = spawn_server(listener, fn client_sock -> hold_client(client_sock) end)

      {:ok, conn} = Tcp.connect("127.0.0.1", port, timeout: 1_000)

      started = System.monotonic_time(:millisecond)
      assert {:error, %Error{code: :timeout}} = Tcp.command(conn, <<"req">>, 80)
      elapsed = System.monotonic_time(:millisecond) - started

      # Deadline must have fired, not run to the generous connect timeout.
      assert elapsed < 500,
             "expected read to fail near 80 ms deadline, took #{elapsed} ms"

      :ok = Tcp.close(conn)
      stop_server(server)
    end

    test "returns the full body when the server answers before the deadline", %{
      listener: listener,
      port: port
    } do
      body = <<10, 20, 30, 40>>
      reply = header(body) <> body

      server =
        spawn_server(listener, fn client_sock ->
          # Consume one request then send the prepared reply.
          {:ok, _} = :gen_tcp.recv(client_sock, 0, 1_000)
          :ok = :gen_tcp.send(client_sock, reply)
          hold_client(client_sock)
        end)

      {:ok, conn} = Tcp.connect("127.0.0.1", port, timeout: 1_000)

      assert {:ok, ^body} = Tcp.command(conn, <<"req">>, 500)

      :ok = Tcp.close(conn)
      stop_server(server)
    end
  end

  describe "command/3 fragmentation" do
    # Tier 1.5 Task 5 — passive `{:packet, :raw}` + `:gen_tcp.recv(socket,
    # N, _)` already handles server-side TCP fragmentation transparently
    # (see notes.md Finding 7). This test pins that guarantee so a future
    # change to the recv shape — a switch to active mode, a buffering
    # refactor — cannot silently regress it.
    test "reassembles a reply split across multiple sends", %{
      listener: listener,
      port: port
    } do
      body = <<1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12>>
      full_reply = header(body) <> body
      # Split inside the header so both `recv_exact` calls must survive
      # fragmentation, not just the body recv.
      <<first::binary-size(4), rest::binary>> = full_reply

      server =
        spawn_server(listener, fn client_sock ->
          {:ok, _} = :gen_tcp.recv(client_sock, 0, 1_000)
          :ok = :gen_tcp.send(client_sock, first)
          # Small delay so the client's first `recv` is likely to be
          # woken by the partial arrival before the rest lands. On a
          # loopback socket this is best-effort, but the assertion does
          # not depend on timing — only on reassembly correctness.
          Process.sleep(20)
          :ok = :gen_tcp.send(client_sock, rest)
          hold_client(client_sock)
        end)

      {:ok, conn} = Tcp.connect("127.0.0.1", port, timeout: 1_000)

      assert {:ok, ^body} = Tcp.command(conn, <<"req">>, 1_000)

      :ok = Tcp.close(conn)
      stop_server(server)
    end
  end

  # Build a valid 8-byte AS proto header for a given body. Matches
  # `Aerospike.Protocol.Message.encode_header/3` for version 2, type 3
  # (AS_MSG). The body content does not matter for `command/3` — the
  # transport returns whatever bytes it reads after the header.
  defp header(body) do
    import Bitwise
    length = byte_size(body)
    proto_word = length ||| 2 <<< 56 ||| 3 <<< 48
    <<proto_word::64-big>>
  end

  defp spawn_server(listener, client_fn) do
    parent = self()

    spawn_link(fn ->
      case :gen_tcp.accept(listener, 2_000) do
        {:ok, client_sock} ->
          send(parent, {:accepted, self()})
          client_fn.(client_sock)

        {:error, _} ->
          :ok
      end
    end)
  end

  # Keep the client socket open and blocked so the transport's `recv`
  # trips the read deadline rather than observing a premature close.
  defp hold_client(client_sock) do
    receive do
      :stop -> :gen_tcp.close(client_sock)
    after
      5_000 -> :gen_tcp.close(client_sock)
    end
  end

  defp stop_server(server) do
    if Process.alive?(server), do: send(server, :stop)
    :ok
  end
end
