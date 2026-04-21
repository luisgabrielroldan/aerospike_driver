defmodule Aerospike.Transport.FakeStreamTest do
  use ExUnit.Case, async: true

  alias Aerospike.Error
  alias Aerospike.Transport.Fake

  describe "stream_open/4 and stream_read/2" do
    setup :fake_with_a1

    test "delivers scripted frames in order and terminates on :done", %{conn: conn, fake: fake} do
      Fake.script_stream(fake, "A1", {:ok, [{:frame, <<1, 2>>}, {:frame, <<3>>}, :done]})

      assert {:ok, stream} = Fake.stream_open(conn, <<"req">>, 1_000, [])
      assert {:ok, <<1, 2>>} = Fake.stream_read(stream, 250)
      assert {:ok, <<3>>} = Fake.stream_read(stream, 250)
      assert :done = Fake.stream_read(stream, 250)
      assert {:error, %Error{code: :network_error}} = Fake.stream_read(stream, 250)
    end

    test "surfaces scripted read failures and closes the stream", %{conn: conn, fake: fake} do
      err = %Error{code: :timeout, message: "scripted stream failure"}
      Fake.script_stream(fake, "A1", {:ok, [{:frame, <<9>>}, {:error, err}]})

      assert {:ok, stream} = Fake.stream_open(conn, <<"req">>, 1_000, [])
      assert {:ok, <<9>>} = Fake.stream_read(stream, 250)
      assert {:error, ^err} = Fake.stream_read(stream, 250)
      assert {:error, %Error{code: :network_error}} = Fake.stream_read(stream, 250)
    end
  end

  describe "stream_close/1" do
    setup :fake_with_a1

    test "is idempotent and makes the handle unusable", %{conn: conn, fake: fake} do
      Fake.script_stream(fake, "A1", {:ok, [{:frame, <<42>>}, :done]})

      assert {:ok, stream} = Fake.stream_open(conn, <<"req">>, 1_000, [])
      assert {:ok, <<42>>} = Fake.stream_read(stream, 250)
      assert Fake.stream_close_count(fake, "A1") == 0

      assert :ok = Fake.stream_close(stream)
      assert Fake.stream_close_count(fake, "A1") == 1

      assert :ok = Fake.stream_close(stream)
      assert Fake.stream_close_count(fake, "A1") == 1
      assert {:error, %Error{code: :network_error}} = Fake.stream_read(stream, 250)
    end
  end

  defp fake_with_a1(_ctx) do
    {:ok, fake} = Fake.start_link(nodes: [{"A1", "10.0.0.1", 3000}])
    {:ok, conn} = Fake.connect("10.0.0.1", 3000, fake: fake)
    %{fake: fake, conn: conn}
  end
end
