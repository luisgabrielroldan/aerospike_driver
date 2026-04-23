defmodule Aerospike.Runtime.StreamingExecutorTest do
  use ExUnit.Case, async: true

  alias Aerospike.Command.StreamingCommand
  alias Aerospike.Error
  alias Aerospike.Policy
  alias Aerospike.Protocol.Message
  alias Aerospike.Query
  alias Aerospike.Runtime.StreamingExecutor

  defmodule TransportStub do
    def put_script(script) do
      Process.put(:transport_script, script)
      Process.put(:transport_open, nil)
      Process.put(:transport_close_count, 0)
    end

    def stream_open_call, do: Process.get(:transport_open)
    def close_count, do: Process.get(:transport_close_count, 0)

    def connect(host, port, opts) do
      Process.put(:transport_connect, {host, port, opts})
      Map.get(script(), :connect, {:ok, :conn})
    end

    def stream_open(conn, request, timeout, opts) do
      Process.put(:transport_open, {conn, IO.iodata_to_binary(request), timeout, opts})
      Map.get(script(), :stream_open, {:ok, :stream})
    end

    def stream_read(_stream, _timeout) do
      case Map.get(script(), :stream_reads, []) do
        [next | rest] ->
          Process.put(:transport_script, Map.put(script(), :stream_reads, rest))
          next

        [] ->
          :done
      end
    end

    def stream_close(_stream) do
      Process.put(:transport_close_count, close_count() + 1)
      :ok
    end

    defp script, do: Process.get(:transport_script, %{})
  end

  test "shared executor runs the stream lifecycle and closes the stream on success" do
    TransportStub.put_script(%{
      stream_reads: [
        {:ok, Message.encode(2, 3, "first")},
        {:ok, Message.encode(2, 3, "second")},
        :done
      ]
    })

    assert {:ok, ["first", "second"]} =
             StreamingExecutor.run_command(test_command(), runtime(), ctx())

    assert {:conn, "wire:A1", 50, [use_compression: true, attempt: 0]} =
             TransportStub.stream_open_call()

    assert TransportStub.close_count() == 1
  end

  test "shared executor closes the stream when frame decoding fails" do
    TransportStub.put_script(%{
      stream_reads: [{:ok, Message.encode(2, 1, "info-frame")}]
    })

    assert {:error, %Error{code: :parse_error}} =
             StreamingExecutor.run_command(test_command(), runtime(), ctx())

    assert TransportStub.close_count() == 1
  end

  test "breaker refusal stops before transport connect" do
    TransportStub.put_script(%{})

    assert {:error, %Error{code: :circuit_open}} =
             StreamingExecutor.run_command(test_command(), runtime(), %{
               scannable: scannable(),
               node_request: node_request(),
               resolve_handle: fn :fake_tender, "A1" -> {:ok, fake_handle()} end,
               allow_dispatch: fn _counters, _breaker ->
                 {:error, Error.from_result_code(:circuit_open)}
               end
             })

    assert TransportStub.stream_open_call() == nil
    assert TransportStub.close_count() == 0
  end

  test "run_node_requests preserves request order through the shared task fan-out" do
    TransportStub.put_script(%{
      stream_reads: [
        {:ok, Message.encode(2, 3, "A1")},
        :done,
        {:ok, Message.encode(2, 3, "B1")},
        :done
      ]
    })

    results =
      StreamingExecutor.run_node_requests(
        test_command(),
        runtime(),
        scannable(),
        [
          node_request(),
          %{node_request() | node_name: "B1"}
        ],
        %Policy.ScanQueryRuntime{
          timeout: 50,
          task_timeout: 100,
          pool_checkout_timeout: 25,
          max_concurrent_nodes: 1,
          task_id: 7,
          cursor: nil
        },
        ctx_overrides: %{
          resolve_handle: fn :fake_tender, node_name ->
            {:ok,
             fake_handle(host: "127.0.0.1", port: if(node_name == "A1", do: 3000, else: 3001))}
          end,
          allow_dispatch: fn _counters, _breaker -> :ok end
        },
        task_runner: fn node_requests, node_fun, _opts ->
          Stream.map(node_requests, fn node_request -> {:ok, node_fun.(node_request)} end)
        end
      )
      |> Enum.to_list()

    assert [{:ok, {:ok, ["A1"]}}, {:ok, {:ok, ["B1"]}}] = results
  end

  defp test_command do
    StreamingCommand.new!(
      name: __MODULE__,
      build_request: fn %{node_request: %{node_name: node_name}} -> "wire:" <> node_name end,
      init: fn _input -> [] end,
      consume_frame: fn body, _input, acc -> {:cont, [body | acc]} end,
      finish: fn acc, _input -> {:ok, Enum.reverse(acc)} end,
      error_result: fn err, _input -> {:error, err} end
    )
  end

  defp runtime do
    %{
      tender: :fake_tender,
      transport: TransportStub
    }
  end

  defp ctx do
    %{
      scannable: scannable(),
      node_request: node_request(),
      resolve_handle: fn :fake_tender, "A1" -> {:ok, fake_handle()} end,
      allow_dispatch: fn _counters, _breaker -> :ok end
    }
  end

  defp scannable do
    Query.new("test", "streaming")
  end

  defp node_request do
    %{
      node_name: "A1",
      node_partitions: :unused,
      policy: %Policy.ScanQueryRuntime{
        timeout: 50,
        task_timeout: 100,
        pool_checkout_timeout: 25,
        max_concurrent_nodes: 0,
        task_id: 7,
        cursor: nil
      }
    }
  end

  defp fake_handle(overrides \\ []) do
    Map.merge(
      %{
        counters: :fake_counters,
        breaker: :fake_breaker,
        host: "127.0.0.1",
        port: 3000,
        connect_opts: [fake: :transport],
        use_compression: true
      },
      Map.new(overrides)
    )
  end
end
