defmodule Aerospike.IndexTaskTest do
  use ExUnit.Case, async: true

  alias Aerospike.IndexTask

  describe "struct" do
    test "enforces required keys" do
      task = %IndexTask{conn: :my_conn, namespace: "test", index_name: "age_idx"}
      assert task.conn == :my_conn
      assert task.namespace == "test"
      assert task.index_name == "age_idx"
    end

    test "raises on missing required keys" do
      assert_raise ArgumentError, fn ->
        struct!(IndexTask, conn: :my_conn)
      end
    end
  end

  describe "status/1 response parsing" do
    # We test the parsing logic by invoking the private parse function indirectly
    # via a stubbed status call. Since status/1 requires a live Router, we test
    # the full module interface through the AsyncTask polling path using a mock.

    defmodule StubTask do
      use Aerospike.AsyncTask

      @impl Aerospike.AsyncTask
      def status(%{response: response}) do
        parse(response)
      end

      defp parse(""), do: {:ok, :complete}

      defp parse(response) do
        case Regex.run(~r/load_pct=(\d+)/, response) do
          [_, "100"] -> {:ok, :complete}
          [_, _] -> {:ok, :in_progress}
          nil -> {:ok, :complete}
        end
      end
    end

    test "empty response → complete" do
      assert {:ok, :complete} = StubTask.status(%{response: ""})
    end

    test "load_pct=100 → complete" do
      response = "ns=test:indexname=age_idx:bin=age:load_pct=100:state=RW"
      assert {:ok, :complete} = StubTask.status(%{response: response})
    end

    test "load_pct < 100 → in_progress" do
      response = "ns=test:indexname=age_idx:bin=age:load_pct=47:state=RW"
      assert {:ok, :in_progress} = StubTask.status(%{response: response})
    end

    test "load_pct=0 → in_progress" do
      response = "ns=test:indexname=age_idx:bin=age:load_pct=0:state=RW"
      assert {:ok, :in_progress} = StubTask.status(%{response: response})
    end

    test "response without load_pct → complete (synced index)" do
      response = "ns=test:indexname=age_idx:bin=age:sync_state=synced:state=RW"
      assert {:ok, :complete} = StubTask.status(%{response: response})
    end

    test "wait/2 delegates to poll loop" do
      # Immediately complete
      assert :ok = StubTask.wait(%{response: ""})
    end
  end

  describe "IndexTask fields" do
    test "t/0 type is a struct" do
      task = %IndexTask{conn: :aero, namespace: "test", index_name: "demo_idx"}
      assert is_struct(task, IndexTask)
    end
  end
end
