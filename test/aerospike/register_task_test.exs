defmodule Aerospike.RegisterTaskTest do
  use ExUnit.Case, async: true

  alias Aerospike.RegisterTask

  describe "struct" do
    test "enforces required keys" do
      task = %RegisterTask{conn: :my_conn, package_name: "my_module.lua"}
      assert task.conn == :my_conn
      assert task.package_name == "my_module.lua"
    end

    test "raises on missing required keys" do
      assert_raise ArgumentError, fn ->
        struct!(RegisterTask, conn: :my_conn)
      end
    end
  end

  describe "status/1 response parsing" do
    defmodule StubUdfTask do
      use Aerospike.AsyncTask

      @impl Aerospike.AsyncTask
      def status(%{response: response, package_name: pkg}) do
        parse_udf_list(response, pkg)
      end

      defp parse_udf_list("", _package_name), do: {:ok, :in_progress}

      defp parse_udf_list(response, package_name) do
        entries = String.split(response, ";", trim: true)

        registered =
          Enum.any?(entries, fn entry ->
            String.contains?(entry, "filename=#{package_name}")
          end)

        if registered, do: {:ok, :complete}, else: {:ok, :in_progress}
      end
    end

    test "empty response -> in_progress" do
      assert {:ok, :in_progress} = StubUdfTask.status(%{response: "", package_name: "test.lua"})
    end

    test "response with matching package -> complete" do
      response = "filename=test.lua,hash=abc123,type=LUA;filename=other.lua,hash=def456,type=LUA;"

      assert {:ok, :complete} =
               StubUdfTask.status(%{response: response, package_name: "test.lua"})
    end

    test "response without matching package -> in_progress" do
      response = "filename=other.lua,hash=def456,type=LUA;"

      assert {:ok, :in_progress} =
               StubUdfTask.status(%{response: response, package_name: "test.lua"})
    end

    test "partial filename match does not count" do
      response = "filename=test_extra.lua,hash=abc123,type=LUA;"

      assert {:ok, :in_progress} =
               StubUdfTask.status(%{response: response, package_name: "test.lua"})
    end

    test "wait/2 delegates to poll loop when immediately complete" do
      assert :ok =
               StubUdfTask.wait(%{
                 response: "filename=test.lua,hash=abc,type=LUA;",
                 package_name: "test.lua"
               })
    end
  end

  describe "RegisterTask fields" do
    test "t/0 type is a struct" do
      task = %RegisterTask{conn: :aero, package_name: "demo.lua"}
      assert is_struct(task, RegisterTask)
    end
  end
end
