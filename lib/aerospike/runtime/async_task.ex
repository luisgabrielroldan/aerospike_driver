defmodule Aerospike.Runtime.AsyncTask do
  @moduledoc """
  Behaviour for pollable async server operations.

  Some Aerospike operations complete asynchronously on the server side.
  `Aerospike.Runtime.AsyncTask` provides the shared `status/1` and `wait/2`
  contract for those task handles without embedding a bespoke polling loop
  in each feature module.
  """

  @doc """
  Blocks until the async operation completes or the timeout is exceeded.
  """
  @callback wait(struct(), keyword()) :: :ok | {:error, Aerospike.Error.t()}

  @doc """
  Returns the current status of the async operation.
  """
  @callback status(struct()) :: {:ok, :complete | :in_progress} | {:error, Aerospike.Error.t()}

  @doc false
  defmacro __using__(_opts) do
    quote do
      alias Aerospike.Runtime.AsyncTask

      @behaviour AsyncTask

      @impl AsyncTask
      def wait(task, opts \\ []) do
        AsyncTask.poll(__MODULE__, task, opts)
      end

      defoverridable wait: 2
    end
  end

  @doc false
  def poll(module, task, opts) do
    poll_interval = Keyword.get(opts, :poll_interval, 1_000)
    timeout_ms = Keyword.get(opts, :timeout, nil)
    deadline = if timeout_ms, do: System.monotonic_time(:millisecond) + timeout_ms, else: nil
    do_poll(module, task, poll_interval, deadline)
  end

  defp do_poll(module, task, poll_interval, deadline) do
    case module.status(task) do
      {:ok, :complete} ->
        :ok

      {:ok, :in_progress} ->
        if deadline && System.monotonic_time(:millisecond) >= deadline do
          {:error, Aerospike.Error.from_result_code(:timeout)}
        else
          Process.sleep(poll_interval)
          do_poll(module, task, poll_interval, deadline)
        end

      {:error, _} = error ->
        error
    end
  end
end
