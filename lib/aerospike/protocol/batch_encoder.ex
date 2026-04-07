defmodule Aerospike.Protocol.BatchEncoder do
  @moduledoc false

  # Encodes batch-any requests (BATCH_INDEX field, type 41) for multi-key commands.

  import Bitwise

  alias Aerospike.Batch
  alias Aerospike.Exp
  alias Aerospike.Key
  alias Aerospike.Protocol.AsmMsg
  alias Aerospike.Protocol.AsmMsg.Field
  alias Aerospike.Protocol.AsmMsg.Operation
  alias Aerospike.Protocol.AsmMsg.Value
  alias Aerospike.Protocol.Message
  alias Aerospike.Protocol.MessagePack
  alias Aerospike.Protocol.OperateFlags

  @batch_msg_repeat 0x01
  @batch_msg_info 0x02
  @batch_msg_gen 0x04
  @batch_msg_ttl 0x08

  @doc false
  @spec encode_batch_get([{non_neg_integer(), Key.t()}], keyword()) :: binary()
  def encode_batch_get(indexed_keys, merged_opts) when is_list(indexed_keys) do
    {read_attr, bin_ops} = read_attr_and_ops_for_get(merged_opts)
    op_count = length(bin_ops)

    entry_fn = fn %Key{} = key, prev ->
      read_entry_bytes(key, read_attr, op_count, bin_ops, merged_opts, prev)
    end

    body = batch_index_body_keys(indexed_keys, merged_opts, entry_fn)
    wrap_batch_message(body, merged_opts)
  end

  @doc false
  @spec encode_batch_exists([{non_neg_integer(), Key.t()}], keyword()) :: binary()
  def encode_batch_exists(indexed_keys, merged_opts) when is_list(indexed_keys) do
    read_attr = AsmMsg.info1_read() ||| AsmMsg.info1_nobindata()

    entry_fn = fn %Key{} = key, prev ->
      read_entry_bytes(key, read_attr, 0, [], merged_opts, prev)
    end

    body = batch_index_body_keys(indexed_keys, merged_opts, entry_fn)
    wrap_batch_message(body, merged_opts)
  end

  defp read_attr_and_ops_for_get(opts) do
    cond do
      Keyword.get(opts, :header_only) == true ->
        {AsmMsg.info1_read() ||| AsmMsg.info1_nobindata(), []}

      is_list(Keyword.get(opts, :bins)) and Keyword.get(opts, :bins) != [] ->
        ops =
          opts
          |> Keyword.fetch!(:bins)
          |> Enum.map(&bin_to_string/1)
          |> Enum.sort()
          |> Enum.map(&Operation.read/1)

        {AsmMsg.info1_read(), ops}

      true ->
        {AsmMsg.info1_read() ||| AsmMsg.info1_get_all(), []}
    end
  end

  defp bin_to_string(b) when is_binary(b), do: b
  defp bin_to_string(a) when is_atom(a), do: Atom.to_string(a)

  defp read_entry_bytes(key, read_attr, op_count, bin_ops, opts, prev) do
    if repeat_ns_set?(prev, key, false, opts) do
      <<@batch_msg_repeat::8>>
    else
      exp = Keyword.get(opts, :read_touch_ttl_percent, 0)
      fields_part = batch_key_fields(key, 0, op_count, opts)

      ops_bin =
        if op_count == 0,
          do: <<>>,
          else: bin_ops |> Enum.map(&Operation.encode/1) |> IO.iodata_to_binary()

      <<
        @batch_msg_info ||| @batch_msg_ttl::8,
        read_attr::8,
        0::8,
        0::8,
        exp::32-big,
        fields_part::binary,
        ops_bin::binary
      >>
    end
  end

  defp repeat_ns_set?(%Key{} = a, %Key{} = b, send_key?, opts) do
    send_key? == Keyword.get(opts, :send_key, false) and a.namespace == b.namespace and
      a.set == b.set
  end

  defp repeat_ns_set?(nil, _b, _, _), do: false

  @doc false
  @spec encode_batch_operate([{non_neg_integer(), Batch.t()}], keyword()) :: binary()
  def encode_batch_operate(indexed_ops, merged_opts) when is_list(indexed_ops) do
    body = batch_index_body_operate(indexed_ops, merged_opts)
    wrap_batch_message(body, merged_opts)
  end

  defp batch_index_body_keys(indexed_keys, merged_opts, entry_fn) do
    max = length(indexed_keys)
    flags = batch_flags_byte(merged_opts)

    {entries_io, _} =
      Enum.reduce(indexed_keys, {[], nil}, fn {idx, key}, {acc, prev} ->
        inner = entry_fn.(key, prev)
        {[acc, <<idx::32-big, key.digest::binary>>, inner], key}
      end)

    entries_bin = IO.iodata_to_binary(entries_io)
    <<max::32-big, flags::8, entries_bin::binary>>
  end

  defp batch_index_body_operate(indexed_ops, global_opts) do
    max = length(indexed_ops)
    flags = batch_flags_byte(global_opts)

    # Do not carry `prev` across heterogeneous records. Reusing the batch "repeat"
    # byte between e.g. read → write would inherit the wrong sub-message shape and
    # the server can mis-handle later ops (writes/deletes appearing to succeed locally
    # while the record is unchanged).
    entries_bin =
      indexed_ops
      |> Enum.map(fn {idx, batch_rec} ->
        {chunk, _} = operate_chunk(idx, batch_rec, global_opts, nil)
        chunk
      end)
      |> IO.iodata_to_binary()

    <<max::32-big, flags::8, entries_bin::binary>>
  end

  defp operate_chunk(idx, %Batch.Read{key: key, opts: o}, global_opts, prev) do
    merged = Keyword.merge(global_opts, o)
    {read_attr, bin_ops} = read_attr_and_ops_for_get(merged)
    op_count = length(bin_ops)

    inner =
      if repeat_ns_set?(prev, key, false, merged) do
        <<@batch_msg_repeat::8>>
      else
        read_entry_bytes(key, read_attr, op_count, bin_ops, merged, nil)
      end

    {<<idx::32-big, key.digest::binary, inner::binary>>, key}
  end

  defp operate_chunk(idx, %Batch.Put{key: key, bins: bins, opts: o}, global_opts, prev) do
    merged = Keyword.merge(global_opts, o)
    ops = Value.encode_bin_operations(bins)

    inner =
      if repeat_ns_set?(prev, key, Keyword.get(merged, :send_key, false), merged) do
        <<@batch_msg_repeat::8>>
      else
        {r, w, i, gen, exp} = put_write_tuple(merged)
        fields = batch_key_fields(key, 0, length(ops), merged)
        ops_bin = ops |> Enum.map(&Operation.encode/1) |> IO.iodata_to_binary()

        <<
          @batch_msg_info ||| @batch_msg_gen ||| @batch_msg_ttl::8,
          r::8,
          w::8,
          i::8,
          gen::16-big,
          exp::32-big,
          fields::binary,
          ops_bin::binary
        >>
      end

    {<<idx::32-big, key.digest::binary, inner::binary>>, key}
  end

  defp operate_chunk(idx, %Batch.Delete{key: key, opts: o}, global_opts, prev) do
    merged = Keyword.merge(global_opts, o)

    inner =
      if repeat_ns_set?(prev, key, Keyword.get(merged, :send_key, false), merged) do
        <<@batch_msg_repeat::8>>
      else
        w =
          AsmMsg.info2_write() ||| AsmMsg.info2_respond_all_ops() ||| AsmMsg.info2_delete()

        w =
          if Keyword.get(merged, :durable_delete),
            do: w ||| AsmMsg.info2_durable_delete(),
            else: w

        fields = batch_key_fields(key, 0, 0, merged)

        <<
          @batch_msg_info ||| @batch_msg_gen ||| @batch_msg_ttl::8,
          0::8,
          w::8,
          0::8,
          0::16-big,
          0::32-big,
          fields::binary
        >>
      end

    {<<idx::32-big, key.digest::binary, inner::binary>>, key}
  end

  defp operate_chunk(idx, %Batch.Operate{key: key, ops: ops, opts: o}, global_opts, prev) do
    merged = Keyword.merge(global_opts, o)
    st = OperateFlags.scan_ops(ops)

    inner =
      if repeat_ns_set?(prev, key, Keyword.get(merged, :send_key, false), merged) do
        <<@batch_msg_repeat::8>>
      else
        operate_inner_bytes(key, ops, st, merged)
      end

    {<<idx::32-big, key.digest::binary, inner::binary>>, key}
  end

  defp operate_chunk(
         idx,
         %Batch.UDF{key: key, package: pkg, function: f, args: args, opts: o},
         global_opts,
         prev
       ) do
    merged = Keyword.merge(global_opts, o)

    inner =
      if repeat_ns_set?(prev, key, Keyword.get(merged, :send_key, false), merged) do
        <<@batch_msg_repeat::8>>
      else
        w = AsmMsg.info2_write()

        w =
          if Keyword.get(merged, :durable_delete),
            do: w ||| AsmMsg.info2_durable_delete(),
            else: w

        i = if Keyword.get(merged, :commit_master), do: AsmMsg.info3_commit_master(), else: 0
        exp = Keyword.get(merged, :ttl, 0)
        fields_inner = batch_key_fields(key, 3, 0, merged)
        udf_fields = udf_extra_fields(pkg, f, args)

        <<
          @batch_msg_info ||| @batch_msg_gen ||| @batch_msg_ttl::8,
          0::8,
          w::8,
          i::8,
          0::16-big,
          exp::32-big,
          fields_inner::binary,
          udf_fields::binary
        >>
      end

    {<<idx::32-big, key.digest::binary, inner::binary>>, key}
  end

  defp operate_inner_bytes(key, ops, st, merged) do
    info1 =
      if st.header_only? do
        st.info1 ||| AsmMsg.info1_nobindata()
      else
        st.info1
      end

    info2 = if st.has_write?, do: st.info2 ||| AsmMsg.info2_write(), else: st.info2

    info2 =
      if respond_all_ops?(merged, st, info1) do
        info2 ||| AsmMsg.info2_respond_all_ops()
      else
        info2
      end

    ops_bin = ops |> Enum.map(&Operation.encode/1) |> IO.iodata_to_binary()
    fields = batch_key_fields(key, 0, length(ops), merged)

    if st.has_write? do
      r = if st.read_bin? or st.read_header?, do: info1, else: 0
      {w, i, gen} = inner_write_flags(merged, info2)
      exp_rw = Keyword.get(merged, :ttl, 0)

      <<
        @batch_msg_info ||| @batch_msg_gen ||| @batch_msg_ttl::8,
        r::8,
        w::8,
        i::8,
        gen::16-big,
        exp_rw::32-big,
        fields::binary,
        ops_bin::binary
      >>
    else
      exp = Keyword.get(merged, :read_touch_ttl_percent, 0)

      <<
        @batch_msg_info ||| @batch_msg_ttl::8,
        info1::8,
        0::8,
        0::8,
        exp::32-big,
        fields::binary,
        ops_bin::binary
      >>
    end
  end

  defp inner_write_flags(merged, w0) do
    w =
      w0
      |> maybe_gen_bits(merged)
      |> maybe_durable(merged)
      |> maybe_create_only(merged)

    i =
      case Keyword.get(merged, :exists) do
        :update_only -> AsmMsg.info3_update_only()
        :replace_only -> AsmMsg.info3_replace_only()
        :create_or_replace -> AsmMsg.info3_create_or_replace()
        _ -> 0
      end

    gen =
      case Keyword.get(merged, :generation) do
        g when is_integer(g) -> g
        _ -> 0
      end

    {w, i, gen}
  end

  defp maybe_gen_bits(w, opts) do
    g = Keyword.get(opts, :generation)

    pol =
      case Keyword.get(opts, :gen_policy) do
        nil when is_integer(g) -> :expect_gen_equal
        nil -> :none
        other -> other
      end

    case {pol, g} do
      {:expect_gen_equal, gv} when is_integer(gv) -> w ||| AsmMsg.info2_generation()
      {:expect_gen_gt, gv} when is_integer(gv) -> w ||| AsmMsg.info2_generation_gt()
      _ -> w
    end
  end

  defp maybe_durable(w, opts) do
    if Keyword.get(opts, :durable_delete), do: w ||| AsmMsg.info2_durable_delete(), else: w
  end

  defp maybe_create_only(w, opts) do
    if Keyword.get(opts, :exists) == :create_only, do: w ||| AsmMsg.info2_create_only(), else: w
  end

  defp put_write_tuple(opts) do
    w0 = AsmMsg.info2_write() ||| AsmMsg.info2_respond_all_ops()
    {w, i, gen} = inner_write_flags(opts, w0)
    exp = Keyword.get(opts, :ttl, 0)
    {0, w, i, gen, exp}
  end

  defp respond_all_ops?(merged, st, info1) do
    want? = st.respond_all? or Keyword.get(merged, :respond_per_each_op, false)
    get_all? = (info1 &&& AsmMsg.info1_get_all()) != 0
    want? and not get_all?
  end

  defp udf_extra_fields(package, function, args) do
    arg_body = args |> Enum.map(&pack_udf_arg/1) |> MessagePack.pack!()

    Field.encode(%Field{type: Field.type_udf_package_name(), data: package}) <>
      Field.encode(%Field{type: Field.type_udf_function(), data: function}) <>
      Field.encode(%Field{type: Field.type_udf_arglist(), data: arg_body})
  end

  defp pack_udf_arg(s) when is_binary(s), do: {:particle_string, s}
  defp pack_udf_arg({:bytes, b}) when is_binary(b), do: {:bytes, b}
  defp pack_udf_arg(nil), do: nil
  defp pack_udf_arg(true), do: true
  defp pack_udf_arg(false), do: false
  defp pack_udf_arg(n) when is_integer(n), do: n
  defp pack_udf_arg(f) when is_float(f), do: f
  defp pack_udf_arg(list) when is_list(list), do: Enum.map(list, &pack_udf_arg/1)

  defp pack_udf_arg(%{} = map) do
    Map.new(map, fn {k, v} -> {pack_udf_arg(k), pack_udf_arg(v)} end)
  end

  defp batch_flags_byte(opts) do
    if Keyword.get(opts, :respond_all_keys, true), do: 0x04, else: 0
  end

  defp batch_key_fields(%Key{} = key, extra_fc, op_count, opts) do
    send_key? = Keyword.get(opts, :send_key, false)
    fc_arg = extra_fc + if(send_key? and key.user_key != nil, do: 1, else: 0)
    written_fc = fc_arg + 2

    base =
      <<
        written_fc::16-big,
        op_count::16-big
      >> <>
        Field.encode(Field.namespace(key.namespace)) <>
        Field.encode(Field.set(key.set))

    if send_key? and key.user_key != nil do
      case Field.key_from_user_key(%{user_key: key.user_key}) do
        nil -> base
        kf -> base <> Field.encode(kf)
      end
    else
      base
    end
  end

  defp wrap_batch_message(batch_index_body, opts) do
    timeout = Keyword.get(opts, :timeout, 0)

    filter_bin =
      case Keyword.get(opts, :filter) do
        %Exp{wire: wire} when is_binary(wire) ->
          Field.encode(%Field{type: Field.type_filter_exp(), data: wire})

        _ ->
          <<>>
      end

    field_count = if filter_bin == <<>>, do: 1, else: 2

    as_body =
      <<
        22::8,
        AsmMsg.info1_batch()::8,
        0::8,
        0::8,
        0::8,
        0::8,
        0::32-big,
        0::32-big,
        timeout::32-signed-big,
        field_count::16-big,
        0::16-big
      >> <>
        Field.encode(%Field{type: Field.type_batch_index(), data: batch_index_body}) <>
        filter_bin

    Message.encode_as_msg(as_body)
  end
end
