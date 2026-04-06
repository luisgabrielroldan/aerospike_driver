defmodule Aerospike.AsyncTaskTest do
  use ExUnit.Case, async: true

  alias Aerospike.Error

  # A mock that always reports :complete immediately.
  defmodule ImmediateMock do
    use Aerospike.AsyncTask

    @impl Aerospike.AsyncTask
    def status(_task), do: {:ok, :complete}
  end

  # A mock that uses an Agent to return :in_progress N times, then :complete.
  defmodule CountdownMock do
    use Aerospike.AsyncTask

    @impl Aerospike.AsyncTask
    def status({:countdown, agent}) do
      count = Agent.get_and_update(agent, fn n -> {n, max(n - 1, 0)} end)

      if count > 0 do
        {:ok, :in_progress}
      else
        {:ok, :complete}
      end
    end
  end

  # A mock that always fails with a specific error.
  defmodule ErrorMock do
    use Aerospike.AsyncTask

    @impl Aerospike.AsyncTask
    def status(_task), do: {:error, Error.from_result_code(:server_mem_error)}
  end

  describe "wait/2 with ImmediateMock" do
    test "returns :ok when status is already :complete" do
      assert :ok = ImmediateMock.wait(%{})
    end

    test "returns :ok with explicit opts" do
      assert :ok = ImmediateMock.wait(%{}, poll_interval: 50, timeout: 5_000)
    end
  end

  describe "wait/2 with CountdownMock (polls multiple times)" do
    test "returns :ok after polling through :in_progress states" do
      {:ok, agent} = Agent.start_link(fn -> 2 end)
      task = {:countdown, agent}
      # 2 in_progress, then complete; poll_interval kept short
      assert :ok = CountdownMock.wait(task, poll_interval: 10)
      Agent.stop(agent)
    end

    test "returns timeout error when deadline is exceeded" do
      # Always :in_progress — will time out
      {:ok, agent} = Agent.start_link(fn -> 999 end)
      task = {:countdown, agent}

      result = CountdownMock.wait(task, poll_interval: 10, timeout: 30)

      assert {:error, %Error{code: :timeout}} = result
      Agent.stop(agent)
    end
  end

  describe "wait/2 with ErrorMock" do
    test "propagates errors from status/1" do
      assert {:error, %Error{code: :server_mem_error}} = ErrorMock.wait(%{})
    end
  end

  describe "status/1 callback" do
    test "ImmediateMock.status/1 returns :complete" do
      assert {:ok, :complete} = ImmediateMock.status(%{})
    end

    test "CountdownMock.status/1 returns :in_progress while count > 0" do
      {:ok, agent} = Agent.start_link(fn -> 1 end)
      task = {:countdown, agent}

      assert {:ok, :in_progress} = CountdownMock.status(task)
      assert {:ok, :complete} = CountdownMock.status(task)
      Agent.stop(agent)
    end
  end
end
