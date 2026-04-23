defmodule Aerospike.Integration.CompressionTest do
  @moduledoc """
  Round-trip test for the cluster-level `:use_compression` opt.

  The Fake transport cannot assert on-the-wire bytes because it replaces
  `Aerospike.Transport.Tcp` at the behaviour seam — scripted replies never
  touch `Tcp.command/4`'s compress/inflate branches. This test therefore
  stands up a loopback `:gen_tcp` listener that speaks just enough of the
  Aerospike protocol to drive one tend cycle plus one GET, and asserts:

    * when cluster `use_compression: true` and the stub server's
      `features` reply advertises `compression`, the request that lands on
      the wire is a type-4 (`AS_MSG_COMPRESSED`) frame;
    * the compressed reply the stub returns round-trips through
      `Aerospike.Get` to a decoded `:key_not_found` result;
    * when cluster `use_compression: false`, the wire frame is a plain
      type-3 AS_MSG even if the stub still advertises the capability.

  The stub is intentionally narrow: one seed, one namespace, no peers,
  no retries. The GET path only needs the partition-owner map to pick
  the single stub node, so a trivial `all_master` replicas reply is
  enough.
  """

  use ExUnit.Case, async: false

  @moduletag :integration

  import Bitwise

  alias Aerospike.Error
  alias Aerospike.Key
  alias Aerospike.Protocol.Message
  alias Aerospike.Test.ReplicasFixture

  @namespace "test"
  @node_name "A1STUB"
  # The Tcp transport only wraps payloads larger than a 128-byte
  # threshold. A minimal GET on {"test", "spike", key} is ~66 bytes on
  # the wire, which legitimately skips compression. Padding the set
  # name with filler pushes the encoded request comfortably past the
  # threshold so the compress branch actually fires.
  @long_set "spike_" <> String.duplicate("x", 120)

  setup do
    {:ok, listener} =
      :gen_tcp.listen(0, [:binary, active: false, packet: :raw, reuseaddr: true])

    {:ok, port} = :inet.port(listener)

    on_exit(fn -> _ = :gen_tcp.close(listener) end)

    %{listener: listener, port: port}
  end

  test "cluster-on + server advertises :compression → compressed wire frame, decoded reply",
       %{listener: listener, port: port} do
    received_ref = make_ref()
    stub = spawn_stub(listener, self(), received_ref, features: "compression;pipelining")

    name = :"compression_cluster_on_#{System.unique_integer([:positive])}"

    {:ok, sup} =
      Aerospike.start_link(
        name: name,
        transport: Aerospike.Transport.Tcp,
        hosts: ["127.0.0.1:#{port}"],
        namespaces: [@namespace],
        tend_trigger: :manual,
        pool_size: 1,
        max_retries: 0,
        use_compression: true
      )

    on_exit(fn -> stop_supervisor(sup) end)

    :ok = Aerospike.Tender.tend_now(name)
    assert Aerospike.Tender.ready?(name)

    key = Key.new(@namespace, @long_set, "missing")
    assert {:error, %Error{code: :key_not_found}} = Aerospike.get(name, key)

    # The stub forwards every AS_MSG frame it receives to the test pid.
    assert_receive {^received_ref, :command_request, wire_bytes}, 2_000

    <<header::binary-8, _rest::binary>> = wire_bytes
    assert {:ok, {2, 4, _length}} = Message.decode_header(header)

    stop_stub(stub)
  end

  test "cluster-off → plain wire frame even when server advertises :compression",
       %{listener: listener, port: port} do
    received_ref = make_ref()
    stub = spawn_stub(listener, self(), received_ref, features: "compression")

    name = :"compression_cluster_off_#{System.unique_integer([:positive])}"

    {:ok, sup} =
      Aerospike.start_link(
        name: name,
        transport: Aerospike.Transport.Tcp,
        hosts: ["127.0.0.1:#{port}"],
        namespaces: [@namespace],
        tend_trigger: :manual,
        pool_size: 1,
        max_retries: 0,
        use_compression: false
      )

    on_exit(fn -> stop_supervisor(sup) end)

    :ok = Aerospike.Tender.tend_now(name)
    assert Aerospike.Tender.ready?(name)

    key = Key.new(@namespace, "spike", "missing")
    assert {:error, %Error{code: :key_not_found}} = Aerospike.get(name, key)

    assert_receive {^received_ref, :command_request, wire_bytes}, 2_000
    <<header::binary-8, _rest::binary>> = wire_bytes
    assert {:ok, {2, 3, _length}} = Message.decode_header(header)

    stop_stub(stub)
  end

  test "cluster-on + server omits :compression → plain wire frame",
       %{listener: listener, port: port} do
    received_ref = make_ref()
    stub = spawn_stub(listener, self(), received_ref, features: "pipelining")

    name = :"compression_feature_off_#{System.unique_integer([:positive])}"

    {:ok, sup} =
      Aerospike.start_link(
        name: name,
        transport: Aerospike.Transport.Tcp,
        hosts: ["127.0.0.1:#{port}"],
        namespaces: [@namespace],
        tend_trigger: :manual,
        pool_size: 1,
        max_retries: 0,
        use_compression: true
      )

    on_exit(fn -> stop_supervisor(sup) end)

    :ok = Aerospike.Tender.tend_now(name)
    assert Aerospike.Tender.ready?(name)

    key = Key.new(@namespace, "spike", "missing")
    assert {:error, %Error{code: :key_not_found}} = Aerospike.get(name, key)

    assert_receive {^received_ref, :command_request, wire_bytes}, 2_000
    <<header::binary-8, _rest::binary>> = wire_bytes
    assert {:ok, {2, 3, _length}} = Message.decode_header(header)

    stop_stub(stub)
  end

  ## Stub Aerospike server

  # Accepts every connection that lands on `listener` (Tender info socket
  # plus pool-worker socket) and serves scripted replies until told to
  # stop. `features` controls the value the `features` info key returns;
  # everything else is hard-coded to the minimum a single-seed tend
  # cycle needs.
  defp spawn_stub(listener, report_to, ref, opts) do
    features = Keyword.fetch!(opts, :features)

    spawn_link(fn -> accept_loop(listener, report_to, ref, features) end)
  end

  defp stop_stub(pid) when is_pid(pid) do
    if Process.alive?(pid), do: send(pid, :stop)
    :ok
  end

  defp accept_loop(listener, report_to, ref, features) do
    receive do
      :stop ->
        :ok
    after
      0 -> :ok
    end

    case :gen_tcp.accept(listener, 500) do
      {:ok, client_sock} ->
        spawn_link(fn -> serve_connection(client_sock, report_to, ref, features) end)
        accept_loop(listener, report_to, ref, features)

      {:error, :timeout} ->
        accept_loop(listener, report_to, ref, features)

      {:error, _} ->
        :ok
    end
  end

  defp serve_connection(sock, report_to, ref, features) do
    case recv_frame(sock) do
      {:ok, {2, type, body}} ->
        handle_frame(sock, type, body, report_to, ref, features)
        serve_connection(sock, report_to, ref, features)

      {:error, _} ->
        _ = :gen_tcp.close(sock)
    end
  end

  # Info request: parse the `\n`-separated command list and reply with a
  # canned map. Only the keys the Tender asks for are handled.
  defp handle_frame(sock, 1, body, _report_to, _ref, features) do
    commands =
      body
      |> String.trim_trailing("\n")
      |> String.split("\n", trim: true)

    reply_body = Enum.map_join(commands, "", &info_reply_line(&1, features))

    frame = Message.encode_info(reply_body)
    :ok = :gen_tcp.send(sock, frame)
  end

  # AS_MSG request: forward the raw wire frame to the test pid for
  # assertion, then reply with a compressed AS_MSG carrying a
  # `:key_not_found` body. The proto header + body are re-wrapped
  # verbatim so the test sees what `Tcp.command/4` actually wrote.
  defp handle_frame(sock, 3, body, report_to, ref, _features) do
    frame = typed_header(2, 3, byte_size(body)) <> body
    send(report_to, {ref, :command_request, frame})

    inner_frame = Message.encode_as_msg(key_not_found_body())
    compressed = Message.encode_compressed_payload(inner_frame)
    :ok = :gen_tcp.send(sock, compressed)
  end

  defp handle_frame(sock, 4, body, report_to, ref, _features) do
    frame = typed_header(2, 4, byte_size(body)) <> body
    send(report_to, {ref, :command_request, frame})

    inner_frame = Message.encode_as_msg(key_not_found_body())
    compressed = Message.encode_compressed_payload(inner_frame)
    :ok = :gen_tcp.send(sock, compressed)
  end

  defp recv_frame(sock) do
    with {:ok, header} <- :gen_tcp.recv(sock, 8, 2_000),
         {:ok, {version, type, length}} <- Message.decode_header(header),
         {:ok, body} <- recv_body(sock, length) do
      {:ok, {version, type, body}}
    end
  end

  defp recv_body(_sock, 0), do: {:ok, <<>>}
  defp recv_body(sock, n), do: :gen_tcp.recv(sock, n, 2_000)

  # Scripted single-key info replies. The Tender issues `node/features`
  # on bootstrap, then `partition-generation/cluster-stable`,
  # `peers-clear-std`, and `replicas` per cycle.
  defp info_reply_line("node", _features), do: "node\t#{@node_name}\n"
  defp info_reply_line("features", features), do: "features\t#{features}\n"
  defp info_reply_line("partition-generation", _), do: "partition-generation\t1\n"
  defp info_reply_line("cluster-stable", _), do: "cluster-stable\tdeadbeef\n"
  defp info_reply_line("peers-clear-std", _), do: "peers-clear-std\t0,3000,[]\n"

  defp info_reply_line("replicas", _) do
    "replicas\t#{ReplicasFixture.all_master(@namespace, 1)}\n"
  end

  defp info_reply_line(other, _), do: "#{other}\n"

  # Minimal AS_MSG body advertising `result_code = 2` (`:key_not_found`)
  # with zero fields and zero ops. The parser reads `result_code` from
  # offset 5; everything else can be zero for this path.
  defp key_not_found_body do
    <<22, 0, 0, 0, 0, 2::8, 0::32, 0::32, 0::32, 0::16, 0::16>>
  end

  defp typed_header(version, type, length) do
    proto_word = length ||| version <<< 56 ||| type <<< 48
    <<proto_word::64-big>>
  end

  defp stop_supervisor(sup) do
    Supervisor.stop(sup)
  catch
    :exit, _ -> :ok
  end
end
