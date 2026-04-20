defmodule Aerospike.Transport.FakeTest do
  use ExUnit.Case, async: true

  alias Aerospike.Error
  alias Aerospike.Transport.Fake

  describe "connect/3" do
    test "returns a handle for a registered host/port" do
      {:ok, fake} = Fake.start_link(nodes: [{"A1", "10.0.0.1", 3000}])

      assert {:ok, conn} = Fake.connect("10.0.0.1", 3000, fake: fake)
      assert conn.node_id == "A1"
      assert :ok = Fake.close(conn)
    end

    test "returns :connection_error for an unregistered host" do
      {:ok, fake} = Fake.start_link()

      assert {:error, %Error{code: :connection_error}} =
               Fake.connect("10.0.0.9", 3000, fake: fake)
    end

    test "returns :connection_error when the :fake opt is missing" do
      assert {:error, %Error{code: :connection_error}} =
               Fake.connect("10.0.0.1", 3000, [])
    end
  end

  describe "info/2" do
    setup :fake_with_a1

    test "consumes scripted replies in order", %{conn: conn, fake: fake} do
      Fake.script_info(fake, "A1", ["node"], %{"node" => "BB9A1"})
      Fake.script_info(fake, "A1", ["node"], %{"node" => "BB9A1-changed"})

      assert {:ok, %{"node" => "BB9A1"}} = Fake.info(conn, ["node"])
      assert {:ok, %{"node" => "BB9A1-changed"}} = Fake.info(conn, ["node"])
    end

    test "returns the default reply when the script queue is empty", %{conn: conn} do
      assert {:error, %Error{code: :no_script}} = Fake.info(conn, ["node"])
    end

    test "matches on the command list exactly", %{conn: conn, fake: fake} do
      Fake.script_info(fake, "A1", ["peers-clear-std"], %{"peers-clear-std" => "..."})

      assert {:error, %Error{code: :no_script}} = Fake.info(conn, ["node"])
      assert {:ok, %{"peers-clear-std" => "..."}} = Fake.info(conn, ["peers-clear-std"])
    end

    test "disconnect causes a :network_error without consuming scripts", %{
      conn: conn,
      fake: fake
    } do
      Fake.script_info(fake, "A1", ["node"], %{"node" => "BB9A1"})
      Fake.disconnect(fake, "A1")

      assert {:error, %Error{code: :network_error}} = Fake.info(conn, ["node"])

      Fake.reconnect(fake, "A1")
      assert {:ok, %{"node" => "BB9A1"}} = Fake.info(conn, ["node"])
    end
  end

  describe "command/3" do
    setup :fake_with_a1

    test "returns the next scripted reply regardless of request bytes", %{
      conn: conn,
      fake: fake
    } do
      Fake.script_command(fake, "A1", {:ok, <<1, 2, 3>>})

      assert {:ok, <<1, 2, 3>>} = Fake.command(conn, <<"anything">>, 1_000)
    end

    test "surfaces scripted errors", %{conn: conn, fake: fake} do
      err = %Error{code: :timeout, message: "scripted"}
      Fake.script_command(fake, "A1", {:error, err})

      assert {:error, ^err} = Fake.command(conn, <<>>, 1_000)
    end

    test "records the last deadline passed for assertions", %{conn: conn, fake: fake} do
      Fake.script_command(fake, "A1", {:ok, <<1>>})
      Fake.script_command(fake, "A1", {:ok, <<2>>})

      assert Fake.last_command_deadline(fake, "A1") == nil

      assert {:ok, <<1>>} = Fake.command(conn, <<"req">>, 750)
      assert Fake.last_command_deadline(fake, "A1") == 750

      assert {:ok, <<2>>} = Fake.command(conn, <<"req">>, 1_250)
      assert Fake.last_command_deadline(fake, "A1") == 1_250
    end
  end

  describe "close/1" do
    test "is idempotent" do
      {:ok, fake} = Fake.start_link(nodes: [{"A1", "10.0.0.1", 3000}])
      {:ok, conn} = Fake.connect("10.0.0.1", 3000, fake: fake)

      assert :ok = Fake.close(conn)
      assert :ok = Fake.close(conn)
    end

    test "closed handles return :network_error on info/command" do
      {:ok, fake} = Fake.start_link(nodes: [{"A1", "10.0.0.1", 3000}])
      {:ok, conn} = Fake.connect("10.0.0.1", 3000, fake: fake)
      :ok = Fake.close(conn)

      assert {:error, %Error{code: :network_error}} = Fake.info(conn, ["node"])
      assert {:error, %Error{code: :network_error}} = Fake.command(conn, <<>>, 1_000)
    end
  end

  defp fake_with_a1(_ctx) do
    {:ok, fake} = Fake.start_link(nodes: [{"A1", "10.0.0.1", 3000}])
    {:ok, conn} = Fake.connect("10.0.0.1", 3000, fake: fake)
    %{fake: fake, conn: conn}
  end
end
