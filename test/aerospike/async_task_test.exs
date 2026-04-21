defmodule Aerospike.AsyncTaskTest do
  use ExUnit.Case, async: true

  defmodule StubTask do
    use Aerospike.AsyncTask

    defstruct [:name]

    @impl Aerospike.AsyncTask
    def status(%__MODULE__{name: name}) do
      key = {:async_task_test_statuses, name}

      case Process.get(key, []) do
        [status | rest] ->
          Process.put(key, rest)
          status

        [] ->
          {:ok, :complete}
      end
    end
  end

  test "wait returns once the task reaches complete" do
    key = {:async_task_test_statuses, :complete}
    Process.put(key, [{:ok, :in_progress}, {:ok, :complete}])

    assert :ok = StubTask.wait(%StubTask{name: :complete}, poll_interval: 0, timeout: 100)
    assert [] = Process.get(key, [])
  end

  test "wait times out when the task never completes" do
    key = {:async_task_test_statuses, :timeout}
    Process.put(key, [{:ok, :in_progress}])

    assert {:error, %Aerospike.Error{code: :timeout}} =
             StubTask.wait(%StubTask{name: :timeout}, poll_interval: 0, timeout: 0)
  end
end
