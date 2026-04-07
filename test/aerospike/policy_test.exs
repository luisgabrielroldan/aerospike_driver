defmodule Aerospike.PolicyTest do
  use ExUnit.Case, async: true

  import Bitwise

  alias Aerospike.Key
  alias Aerospike.Policy
  alias Aerospike.Protocol.AsmMsg

  setup do
    key = Key.new("test", "users", "policy-test")
    {:ok, key: key}
  end

  describe "start_schema/0" do
    test "returns a NimbleOptions schema" do
      assert %NimbleOptions{} = Policy.start_schema()
    end
  end

  describe "validate_start/1" do
    test "accepts minimal valid options" do
      assert {:ok, validated} = Policy.validate_start(name: :a, hosts: ["127.0.0.1:3000"])
      assert validated[:name] == :a
      assert validated[:hosts] == ["127.0.0.1:3000"]
    end

    test "rejects missing name" do
      assert {:error, %NimbleOptions.ValidationError{}} = Policy.validate_start(hosts: ["h:1"])
    end

    test "rejects invalid nested defaults" do
      opts = [name: :a, hosts: ["h:1"], defaults: [write: [ttl: -1]]]
      assert {:error, %NimbleOptions.ValidationError{}} = Policy.validate_start(opts)
    end

    test "accepts tls and tls_opts" do
      opts = [
        name: :a,
        hosts: ["127.0.0.1:3000"],
        tls: true,
        tls_opts: [verify: :verify_peer]
      ]

      assert {:ok, validated} = Policy.validate_start(opts)
      assert validated[:tls] == true
      assert validated[:tls_opts] == [verify: :verify_peer]
    end

    test "defaults tls false and tls_opts empty list" do
      assert {:ok, validated} = Policy.validate_start(name: :a, hosts: ["h:1"])
      assert validated[:tls] == false
      assert validated[:tls_opts] == []
    end

    test "rejects non-keyword tls_opts" do
      opts = [name: :a, hosts: ["h:1"], tls_opts: "bad"]
      assert {:error, %NimbleOptions.ValidationError{}} = Policy.validate_start(opts)
    end
  end

  describe "validate_write/1" do
    test "rejects unknown option" do
      assert {:error, %NimbleOptions.ValidationError{}} = Policy.validate_write(foo: 1)
    end
  end

  describe "validate_read/1" do
    test "accepts valid options" do
      assert {:ok, _} = Policy.validate_read(timeout: 100, replica: 0)
    end

    test "accepts replica atoms" do
      for a <- [:master, :sequence, :any] do
        assert {:ok, _} = Policy.validate_read(timeout: 100, replica: a)
      end
    end

    test "rejects empty :bins list" do
      assert {:error, %NimbleOptions.ValidationError{key: :bins}} =
               Policy.validate_read(bins: [])
    end
  end

  describe "validate_batch/1" do
    test "accepts known options" do
      assert {:ok, opts} =
               Policy.validate_batch(
                 timeout: 10,
                 pool_checkout_timeout: 20,
                 replica: 0,
                 respond_all_keys: false
               )

      assert opts[:respond_all_keys] == false
    end

    test "rejects unknown option" do
      assert {:error, %NimbleOptions.ValidationError{}} = Policy.validate_batch(foo: 1)
    end

    test "accepts replica atoms and filter struct" do
      alias Aerospike.Exp

      assert {:ok, _} =
               Policy.validate_batch(
                 replica: :master,
                 filter: Exp.from_wire(<<1, 2, 3>>)
               )
    end
  end

  describe "policy default merge semantics" do
    test "per-call opts override defaults; other keys preserved" do
      defaults = [timeout: 500, replica: 2]
      per_call = [timeout: 200]
      merged = Keyword.merge(defaults, per_call)
      assert merged[:timeout] == 200
      assert merged[:replica] == 2
    end
  end

  describe "apply_write_policy/2" do
    test "sets create_only flag", %{key: key} do
      msg = base_write(key)
      applied = Policy.apply_write_policy(msg, exists: :create_only)
      assert band(applied.info2, AsmMsg.info2_create_only()) == AsmMsg.info2_create_only()
    end

    test "sets update_only on info3", %{key: key} do
      msg = base_write(key)
      applied = Policy.apply_write_policy(msg, exists: :update_only)
      assert band(applied.info3, AsmMsg.info3_update_only()) == AsmMsg.info3_update_only()
    end

    test "sets generation expect when generation and expect_gen_equal", %{key: key} do
      msg = base_write(key)
      applied = Policy.apply_write_policy(msg, generation: 7, gen_policy: :expect_gen_equal)
      assert band(applied.info2, AsmMsg.info2_generation()) == AsmMsg.info2_generation()
      assert applied.generation == 7
    end

    test "defaults to expect_gen_equal when generation given without gen_policy", %{key: key} do
      msg = base_write(key)
      applied = Policy.apply_write_policy(msg, generation: 3)
      assert band(applied.info2, AsmMsg.info2_generation()) == AsmMsg.info2_generation()
      assert applied.generation == 3
    end

    test "sets durable_delete on info2", %{key: key} do
      msg = base_write(key)
      applied = Policy.apply_write_policy(msg, durable_delete: true)
      assert band(applied.info2, AsmMsg.info2_durable_delete()) == AsmMsg.info2_durable_delete()
    end

    test "sets replace_only on info3", %{key: key} do
      msg = base_write(key)
      applied = Policy.apply_write_policy(msg, exists: :replace_only)
      assert band(applied.info3, AsmMsg.info3_replace_only()) == AsmMsg.info3_replace_only()
    end

    test "sets create_or_replace on info3", %{key: key} do
      msg = base_write(key)
      applied = Policy.apply_write_policy(msg, exists: :create_or_replace)

      assert band(applied.info3, AsmMsg.info3_create_or_replace()) ==
               AsmMsg.info3_create_or_replace()
    end

    test "sets expect_gen_gt when gen_policy is expect_gen_gt", %{key: key} do
      msg = base_write(key)
      applied = Policy.apply_write_policy(msg, generation: 9, gen_policy: :expect_gen_gt)
      assert band(applied.info2, AsmMsg.info2_generation_gt()) == AsmMsg.info2_generation_gt()
      assert applied.generation == 9
    end

    test "sets expiration from ttl", %{key: key} do
      msg = base_write(key)
      applied = Policy.apply_write_policy(msg, ttl: 42)
      assert applied.expiration == 42
    end
  end

  describe "validate_start!/1" do
    test "returns validated opts on valid input" do
      opts = [name: :ok, hosts: ["127.0.0.1:3000"]]
      validated = Policy.validate_start!(opts)
      assert validated[:name] == :ok
    end

    test "raises ArgumentError on invalid input" do
      assert_raise ArgumentError, fn ->
        Policy.validate_start!(hosts: ["h"])
      end
    end
  end

  describe "validate_delete/1" do
    test "accepts valid options" do
      assert {:ok, _} = Policy.validate_delete(timeout: 100)
    end

    test "rejects unknown option" do
      assert {:error, %NimbleOptions.ValidationError{}} = Policy.validate_delete(foo: 1)
    end
  end

  describe "validate_exists/1" do
    test "accepts valid options" do
      assert {:ok, _} = Policy.validate_exists(timeout: 100)
    end

    test "rejects unknown option" do
      assert {:error, %NimbleOptions.ValidationError{}} = Policy.validate_exists(foo: 1)
    end
  end

  describe "validate_touch/1" do
    test "accepts valid options" do
      assert {:ok, _} = Policy.validate_touch(ttl: 60)
    end

    test "rejects unknown option" do
      assert {:error, %NimbleOptions.ValidationError{}} = Policy.validate_touch(foo: 1)
    end
  end

  describe "apply_send_key/3" do
    test "appends KEY field for integer user key", %{key: _key} do
      int_key = Key.new("test", "users", 42)
      msg = base_write(int_key)
      applied = Policy.apply_send_key(msg, int_key, send_key: true)
      assert length(applied.fields) == length(msg.fields) + 1
    end

    test "appends KEY field for string user key", %{key: key} do
      msg = base_write(key)
      applied = Policy.apply_send_key(msg, key, send_key: true)
      assert length(applied.fields) == length(msg.fields) + 1
    end

    test "skips KEY field for digest-only key (nil user_key)" do
      d = :crypto.hash(:ripemd160, <<>>)
      digest_key = Key.from_digest("test", "users", d)
      msg = AsmMsg.write_command(digest_key.namespace, digest_key.set, digest_key.digest, [])
      applied = Policy.apply_send_key(msg, digest_key, send_key: true)
      assert length(applied.fields) == length(msg.fields)
    end

    test "does nothing when send_key is false or absent", %{key: key} do
      msg = base_write(key)
      assert Policy.apply_send_key(msg, key, []) == msg
      assert Policy.apply_send_key(msg, key, send_key: false) == msg
    end
  end

  describe "apply_delete_policy/2" do
    test "sets timeout", %{key: key} do
      msg = AsmMsg.delete_command(key.namespace, key.set, key.digest)
      applied = Policy.apply_delete_policy(msg, timeout: 999)
      assert applied.timeout == 999
    end

    test "sets durable_delete on delete", %{key: key} do
      msg = AsmMsg.delete_command(key.namespace, key.set, key.digest)
      applied = Policy.apply_delete_policy(msg, durable_delete: true)
      import Bitwise
      assert band(applied.info2, AsmMsg.info2_durable_delete()) == AsmMsg.info2_durable_delete()
    end
  end

  describe "apply_touch_policy/2" do
    test "sets ttl and timeout", %{key: key} do
      msg = AsmMsg.touch_command(key.namespace, key.set, key.digest)
      applied = Policy.apply_touch_policy(msg, ttl: 100, timeout: 500)
      assert applied.expiration == 100
      assert applied.timeout == 500
    end
  end

  describe "wire-flag policy mappings" do
    test "write gen_policy expect_gen_equal sets info2 generation bit", %{key: key} do
      # info2_generation bit is 0x04.
      msg = base_write(key)
      applied = Policy.apply_write_policy(msg, generation: 11, gen_policy: :expect_gen_equal)
      assert band(applied.info2, AsmMsg.info2_generation()) == AsmMsg.info2_generation()
      assert applied.generation == 11
    end

    test "read header_only sets READ and NOBINDATA bits", %{key: key} do
      # info1_read is 0x01 and info1_nobindata is 0x20.
      msg = Policy.read_message_for_opts(key, header_only: true)
      assert band(msg.info1, AsmMsg.info1_read()) == AsmMsg.info1_read()
      assert band(msg.info1, AsmMsg.info1_nobindata()) == AsmMsg.info1_nobindata()
    end

    test "delete durable_delete sets info2 durable-delete bit", %{key: key} do
      # info2_durable_delete bit is 0x10.
      msg = AsmMsg.delete_command(key.namespace, key.set, key.digest)
      applied = Policy.apply_delete_policy(msg, durable_delete: true)
      assert band(applied.info2, AsmMsg.info2_durable_delete()) == AsmMsg.info2_durable_delete()
    end

    test "operate write path sets info3 update-only bit", %{key: key} do
      # info3_update_only bit is 0x08.
      msg = base_write(key)

      applied =
        Policy.apply_operate_policy(msg, [exists: :update_only, timeout: 777, ttl: 8], true)

      assert band(applied.info3, AsmMsg.info3_update_only()) == AsmMsg.info3_update_only()
      assert applied.timeout == 777
      assert applied.expiration == 8
    end

    test "operate read-only path ignores write-only generation flags", %{key: key} do
      # info2_generation bit is 0x04; read-only operate should not set it.
      msg = AsmMsg.read_command(key.namespace, key.set, key.digest)

      applied =
        Policy.apply_operate_policy(msg, [generation: 9, gen_policy: :expect_gen_equal], false)

      assert band(applied.info2, AsmMsg.info2_generation()) == 0
      assert applied.generation == 0
    end

    test "batch outer timeout maps option into AsmMsg timeout", %{key: key} do
      # timeout is a header field on the outer batch AsmMsg (int32 on wire).
      msg = AsmMsg.read_command(key.namespace, key.set, key.digest)
      applied = Policy.apply_batch_outer_timeout(msg, timeout: 5_432)
      assert applied.timeout == 5_432
    end
  end

  describe "validation_error_message/1" do
    test "formats NimbleOptions error" do
      {:error, e} = Policy.validate_write(bad: 1)
      msg = Policy.validation_error_message(e)
      assert is_binary(msg)
      assert msg != ""
    end
  end

  describe "read_message_for_opts/2" do
    test "header_only uses nobindata", %{key: key} do
      msg = Policy.read_message_for_opts(key, header_only: true)
      expected = AsmMsg.info1_read() ||| AsmMsg.info1_nobindata()
      assert msg.info1 == expected
      assert msg.operations == []
    end

    test "bins list yields read operations", %{key: key} do
      msg = Policy.read_message_for_opts(key, bins: ["z", :a])
      assert msg.operations != []
      assert length(msg.operations) == 2
    end

    test "default read without bins or header_only uses read_command", %{key: key} do
      msg = Policy.read_message_for_opts(key, [])
      expected = AsmMsg.read_command(key.namespace, key.set, key.digest)
      assert msg == expected
    end
  end

  defp base_write(key) do
    AsmMsg.write_command(key.namespace, key.set, key.digest, [])
  end
end
