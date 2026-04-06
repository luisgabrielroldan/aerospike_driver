defmodule Aerospike.AsyncTask do
  @moduledoc """
  Behaviour for pollable async server operations.

  Some Aerospike server operations (secondary index creation, UDF registration) complete
  asynchronously. The server accepts the request immediately and performs the work in the
  background. `AsyncTask` provides a uniform interface for checking and waiting on these
  operations.

  Two modules implement this behaviour:

  - `Aerospike.IndexTask` — tracks secondary index build progress
  - `Aerospike.RegisterTask` — tracks UDF package registration progress

  ## Callbacks

  Implementing modules must define `status/1`. A default `wait/2` implementation is injected
  by `use Aerospike.AsyncTask` and polls `status/1` in a loop; implementors may override it.

  ## Usage

      task = Aerospike.create_index(:my_conn, "test", "demo",
        bin: "age", name: "age_idx", type: :numeric
      )

      # Poll manually
      {:ok, :complete} = Aerospike.IndexTask.status(task)

      # Or block until done
      :ok = Aerospike.IndexTask.wait(task, timeout: 30_000)

  """

  @doc """
  Blocks until the async operation completes or the timeout is exceeded.

  ## Options

  - `:poll_interval` — milliseconds between status checks (default: `1_000`)
  - `:timeout` — maximum milliseconds to wait (default: no timeout)

  Returns `:ok` when complete, `{:error, %Aerospike.Error{}}` on failure or timeout.
  """
  @callback wait(struct(), keyword()) :: :ok | {:error, Aerospike.Error.t()}

  @doc """
  Returns the current status of the async operation.

  Returns `{:ok, :complete}` when finished, `{:ok, :in_progress}` while running,
  or `{:error, %Aerospike.Error{}}` on failure.
  """
  @callback status(struct()) :: {:ok, :complete | :in_progress} | {:error, Aerospike.Error.t()}

  @doc false
  defmacro __using__(_opts) do
    quote do
      @behaviour Aerospike.AsyncTask

      @impl Aerospike.AsyncTask
      def wait(task, opts \\ []) do
        Aerospike.AsyncTask.poll(__MODULE__, task, opts)
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
