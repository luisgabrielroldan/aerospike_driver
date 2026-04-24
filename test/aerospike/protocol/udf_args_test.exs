defmodule Aerospike.Protocol.UdfArgsTest do
  use ExUnit.Case, async: true

  alias Aerospike.Protocol.MessagePack
  alias Aerospike.Protocol.UdfArgs

  test "pack_arg/1 recursively rewrites strings and bytes wrappers" do
    assert UdfArgs.pack_arg(nil) == nil
    assert UdfArgs.pack_arg(true) == true
    assert UdfArgs.pack_arg(false) == false
    assert UdfArgs.pack_arg(7) == 7
    assert UdfArgs.pack_arg(1.5) == 1.5
    assert UdfArgs.pack_arg("abc") == {:particle_string, "abc"}
    assert UdfArgs.pack_arg({:bytes, <<1, 2>>}) == {:bytes, <<1, 2>>}

    assert UdfArgs.pack_arg([1, "abc", {:bytes, <<1>>}]) == [
             1,
             {:particle_string, "abc"},
             {:bytes, <<1>>}
           ]

    assert UdfArgs.pack_arg(%{"name" => "ada", "blob" => {:bytes, <<2>>}}) == %{
             {:particle_string, "name"} => {:particle_string, "ada"},
             {:particle_string, "blob"} => {:bytes, <<2>>}
           }
  end

  test "pack!/1 encodes rewritten values through MessagePack" do
    packed = UdfArgs.pack!([1, "abc", {:bytes, <<7>>}, %{"k" => "v"}])

    assert MessagePack.unpack!(packed) == [1, "abc", <<4, 7>>, %{"k" => "v"}]
  end
end
