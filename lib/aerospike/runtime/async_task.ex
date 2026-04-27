defmodule Aerospike.Runtime.AsyncTask do
  @moduledoc false

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
      @doc """
      Blocks until the async operation completes or the timeout is exceeded.

      Supported options:

        * `:poll_interval` — milliseconds to sleep between status checks.
          Defaults to `1_000`.
        * `:timeout` — maximum milliseconds to wait. When omitted, polling
          continues until the operation completes or returns an error.
      """
      @spec wait(struct()) :: :ok | {:error, Aerospike.Error.t()}
      @spec wait(struct(), keyword()) :: :ok | {:error, Aerospike.Error.t()}
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
