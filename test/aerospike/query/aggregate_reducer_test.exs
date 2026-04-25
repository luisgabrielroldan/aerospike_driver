defmodule Aerospike.Query.AggregateReducerTest do
  use ExUnit.Case, async: true

  alias Aerospike.Error
  alias Aerospike.Query.AggregateReducer

  @sum_source """
  local function reducer(left, right)
    return left + right
  end

  function sum_values(stream)
    return stream : reduce(reducer)
  end
  """

  test "reduces server aggregate values with inline source" do
    assert {:ok, reducer} = AggregateReducer.prepare("pkg", "sum_values", [], source: @sum_source)

    assert AggregateReducer.run(reducer, [1, 2, 3]) == {:ok, 6}
  end

  test "loads source from a local path" do
    path =
      Path.join(System.tmp_dir!(), "aggregate_reducer_#{System.unique_integer([:positive])}.lua")

    File.write!(path, @sum_source)
    on_exit(fn -> File.rm(path) end)

    assert {:ok, reducer} = AggregateReducer.prepare("pkg", "sum_values", [], source_path: path)

    assert AggregateReducer.run(reducer, [4, 5]) == {:ok, 9}
  end

  test "validates source options" do
    assert {:error, %Error{code: :invalid_argument, message: missing}} =
             AggregateReducer.prepare("pkg", "sum_values", [], [])

    assert missing =~ "missing aggregate Lua source"

    assert {:error, %Error{code: :invalid_argument, message: both}} =
             AggregateReducer.prepare("pkg", "sum_values", [],
               source: @sum_source,
               source_path: "/tmp/source.lua"
             )

    assert both =~ "exactly one"

    assert {:error, %Error{code: :invalid_argument, message: non_binary}} =
             AggregateReducer.prepare("pkg", "sum_values", [], source: :bad)

    assert non_binary =~ "must be a binary"

    assert {:error, %Error{code: :invalid_argument, message: unreadable}} =
             AggregateReducer.prepare("pkg", "sum_values", [], source_path: "/no/such/source.lua")

    assert unreadable =~ "unable to read aggregate Lua source"
  end

  test "validates unsupported local arguments before reduction" do
    assert {:error, %Error{code: :invalid_argument, message: message}} =
             AggregateReducer.prepare("pkg", "sum_values", [{:blob, <<1>>}], source: @sum_source)

    assert message =~ "blob values"
  end

  test "rejects unsupported node targeting" do
    assert {:error, %Error{code: :invalid_argument, message: message}} =
             AggregateReducer.prepare("pkg", "sum_values", [], source: @sum_source, node: "A1")

    assert message =~ ":node"
  end

  test "maps local Lua load and missing function errors to query errors" do
    assert {:error, %Error{code: :query_generic, message: bad_source}} =
             AggregateReducer.prepare("pkg", "sum_values", [], source: "function bad(")

    assert bad_source =~ "aggregate Lua execution failed"

    assert {:error, %Error{code: :query_generic, message: missing_function}} =
             AggregateReducer.prepare("pkg", "missing", [], source: @sum_source)

    assert missing_function =~ "aggregate Lua execution failed"
  end

  test "converts maps, lists, and function arguments across the Lua boundary" do
    source = """
    function total_named(stream, name)
      local function reducer(left, right)
        return {
          sum = left[name] + right[name],
          count = left["count"] + right["count"],
          labels = {left["labels"][1], right["labels"][1]}
        }
      end

      return stream : reduce(reducer)
    end
    """

    assert {:ok, reducer} =
             AggregateReducer.prepare("pkg", "total_named", ["score"], source: source)

    values = [
      %{"score" => 3, "count" => 1, "labels" => ["a"]},
      %{"score" => 5, "count" => 1, "labels" => ["b"]}
    ]

    assert AggregateReducer.run(reducer, values) ==
             {:ok, %{"count" => 2, "labels" => ["a", "b"], "sum" => 8}}
  end

  test "returns nil for empty final output" do
    assert {:ok, reducer} = AggregateReducer.prepare("pkg", "sum_values", [], source: @sum_source)

    assert AggregateReducer.run(reducer, []) == {:ok, nil}
  end

  test "rejects multiple final outputs" do
    source = """
    function passthrough(stream)
      return stream : map(function(value) return value end)
    end
    """

    assert {:ok, reducer} = AggregateReducer.prepare("pkg", "passthrough", [], source: source)

    assert {:error, %Error{code: :query_generic, message: message}} =
             AggregateReducer.run(reducer, [1, 2])

    assert message =~ "multiple results"
  end

  test "maps unsupported helpers and runtime errors to query errors" do
    unsupported = """
    function bad(stream)
      return stream : groupby(function(value) return value end)
    end
    """

    assert {:error, %Error{code: :query_generic}} =
             AggregateReducer.prepare("pkg", "bad", [], source: unsupported)

    runtime_error = """
    function bad(stream)
      return stream : reduce(function(_left, _right) error("boom") end)
    end
    """

    assert {:ok, reducer} = AggregateReducer.prepare("pkg", "bad", [], source: runtime_error)

    assert {:error, %Error{code: :query_generic, message: message}} =
             AggregateReducer.run(reducer, [1, 2])

    assert message =~ "aggregate Lua execution failed"
  end

  test "rejects unsupported stream values" do
    assert {:ok, reducer} = AggregateReducer.prepare("pkg", "sum_values", [], source: @sum_source)

    assert {:error, %Error{code: :query_generic, message: message}} =
             AggregateReducer.run(reducer, [{:blob, <<1>>}])

    assert message =~ "blob values"
  end

  test "propagates stream errors without remapping them" do
    assert {:ok, reducer} = AggregateReducer.prepare("pkg", "sum_values", [], source: @sum_source)

    stream =
      Stream.map([:raise], fn :raise ->
        raise Error.from_result_code(:network_error)
      end)

    assert {:error, %Error{code: :network_error}} = AggregateReducer.run(reducer, stream)
  end

  test "times out local reduction" do
    source = """
    function spin(stream)
      return stream : reduce(function(left, right)
        while true do
        end

        return left + right
      end)
    end
    """

    assert {:ok, reducer} =
             AggregateReducer.prepare("pkg", "spin", [], source: source, timeout: 50)

    assert {:error, %Error{code: :timeout}} = AggregateReducer.run(reducer, [1, 2])
  end
end
