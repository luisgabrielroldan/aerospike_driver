defmodule Aerospike.Policy do
  @moduledoc false
  # Validates user-supplied options and translates them into wire-level AsmMsg
  # flags and fields. Each command type (write, read, delete, exists, touch) has
  # its own NimbleOptions schema and an `apply_*_policy/2` function that sets
  # the corresponding info2/info3 flag bits, generation, TTL, and timeout on
  # the outgoing message.

  import Bitwise

  alias Aerospike.Exp
  alias Aerospike.Key
  alias Aerospike.Protocol.AsmMsg
  alias Aerospike.Protocol.AsmMsg.Field
  alias Aerospike.Protocol.AsmMsg.Operation
  alias Aerospike.Tables
  alias Aerospike.Txn

  # Replica routing: atoms match the public API proposal; non-negative integers are the
  # wire/partition-table replica index (0 = master) for advanced use.
  @replica_opt {:or, [{:in, [:master, :sequence, :any]}, :non_neg_integer]}

  # Merges connection-level defaults (set at `start_link`) with per-call opts.
  @doc false
  @spec merge_defaults(atom(), atom(), keyword()) :: keyword()
  def merge_defaults(conn, kind, opts) when is_atom(conn) and is_atom(kind) and is_list(opts) do
    Keyword.merge(read_defaults(conn, kind), opts)
  end

  @doc false
  @spec read_defaults(atom(), atom()) :: keyword()
  def read_defaults(conn, command_type) when is_atom(conn) do
    case :ets.lookup(Tables.meta(conn), :policy_defaults) do
      [{_, defaults}] -> Keyword.get(defaults, command_type, [])
      [] -> []
    end
  end

  # -- NimbleOptions schemas for per-command policies -------------------------

  @txn_opt [txn: [type: {:struct, Txn}, doc: "Transaction handle"]]

  @write_keys [
                ttl: [type: :non_neg_integer],
                timeout: [type: :non_neg_integer],
                generation: [type: :non_neg_integer],
                gen_policy: [type: {:in, [:none, :expect_gen_equal, :expect_gen_gt]}],
                exists: [
                  type: {:in, [:create_only, :update_only, :replace_only, :create_or_replace]}
                ],
                send_key: [type: :boolean],
                durable_delete: [type: :boolean],
                filter: [type: {:struct, Exp}],
                pool_checkout_timeout: [type: :non_neg_integer],
                replica: [type: @replica_opt]
              ] ++ @txn_opt

  @read_keys [
               timeout: [type: :non_neg_integer],
               # Specific bin names to fetch; omit to read all bins.
               bins: [type: {:list, {:or, [:string, :atom]}}],
               # When true, returns generation/expiration metadata without bin data.
               header_only: [type: :boolean],
               # Read touch TTL percent for batch/read policies (0 = default).
               read_touch_ttl_percent: [type: :non_neg_integer],
               filter: [type: {:struct, Exp}],
               pool_checkout_timeout: [type: :non_neg_integer],
               replica: [type: @replica_opt]
             ] ++ @txn_opt

  @delete_keys [
                 timeout: [type: :non_neg_integer],
                 durable_delete: [type: :boolean],
                 filter: [type: {:struct, Exp}],
                 pool_checkout_timeout: [type: :non_neg_integer],
                 replica: [type: @replica_opt]
               ] ++ @txn_opt

  @exists_keys [
                 timeout: [type: :non_neg_integer],
                 filter: [type: {:struct, Exp}],
                 pool_checkout_timeout: [type: :non_neg_integer],
                 replica: [type: @replica_opt]
               ] ++ @txn_opt

  @touch_keys [
                ttl: [type: :non_neg_integer],
                timeout: [type: :non_neg_integer],
                filter: [type: {:struct, Exp}],
                pool_checkout_timeout: [type: :non_neg_integer],
                replica: [type: @replica_opt]
              ] ++ @txn_opt

  # Operate merges read + write semantics; options mirror `put`/`get` where applicable.
  @operate_keys [
                  ttl: [type: :non_neg_integer],
                  timeout: [type: :non_neg_integer],
                  generation: [type: :non_neg_integer],
                  gen_policy: [type: {:in, [:none, :expect_gen_equal, :expect_gen_gt]}],
                  exists: [
                    type: {:in, [:create_only, :update_only, :replace_only, :create_or_replace]}
                  ],
                  send_key: [type: :boolean],
                  durable_delete: [type: :boolean],
                  respond_per_each_op: [type: :boolean],
                  pool_checkout_timeout: [type: :non_neg_integer],
                  replica: [type: @replica_opt]
                ] ++ @txn_opt

  # Batch: outer message timeout, pool checkout, replica routing, server batch flags.
  @batch_keys [
                timeout: [type: :non_neg_integer],
                pool_checkout_timeout: [type: :non_neg_integer],
                replica: [type: @replica_opt],
                respond_all_keys: [type: :boolean, default: true],
                filter: [type: {:struct, Exp}]
              ] ++ @txn_opt

  # Info / admin commands: socket timeout and pool checkout only (no wire-level policy to apply).
  @info_keys [
    timeout: [type: :non_neg_integer],
    pool_checkout_timeout: [type: :non_neg_integer]
  ]

  @index_create_keys [
    bin: [type: :string, required: true, doc: "Bin name to index"],
    name: [type: :string, required: true, doc: "Index name"],
    type: [
      type: {:in, [:numeric, :string, :geo2dsphere]},
      required: true,
      doc: "Index data type"
    ],
    collection: [
      type: {:in, [:list, :mapkeys, :mapvalues]},
      doc: "Collection index type for CDT bins"
    ],
    pool_checkout_timeout: [type: :non_neg_integer]
  ]

  # Scan / query execution (facade + ScanOps): socket timeout, replica routing, pool checkout.
  @scan_keys [
    timeout: [
      type: :non_neg_integer,
      default: 30_000,
      doc: "Socket timeout in milliseconds"
    ],
    pool_checkout_timeout: [type: :non_neg_integer, default: 5_000],
    replica: [type: @replica_opt, default: :master]
  ]

  @query_keys [
    timeout: [type: :non_neg_integer, default: 30_000],
    pool_checkout_timeout: [type: :non_neg_integer, default: 5_000],
    replica: [type: @replica_opt, default: :master]
  ]

  # Per-command defaults that can be set at `Aerospike.start_link/1` time.
  @defaults_keys [
    write: [type: :keyword_list, keys: @write_keys],
    read: [type: :keyword_list, keys: @read_keys],
    delete: [type: :keyword_list, keys: @delete_keys],
    exists: [type: :keyword_list, keys: @exists_keys],
    touch: [type: :keyword_list, keys: @touch_keys],
    operate: [type: :keyword_list, keys: @operate_keys],
    batch: [type: :keyword_list, keys: @batch_keys],
    scan: [type: :keyword_list, keys: @scan_keys],
    query: [type: :keyword_list, keys: @query_keys]
  ]

  # Schema for the top-level `Aerospike.start_link/1` options.
  @start_schema NimbleOptions.new!(
                  name: [type: :atom, required: true],
                  hosts: [type: {:list, :string}, required: true],
                  pool_size: [type: :pos_integer, default: 10],
                  pool_checkout_timeout: [type: :non_neg_integer, default: 5_000],
                  connect_timeout: [type: :non_neg_integer, default: 5_000],
                  tend_interval: [type: :non_neg_integer, default: 1_000],
                  recv_timeout: [type: :non_neg_integer, default: 5_000],
                  auth_opts: [type: :keyword_list, default: []],
                  tls: [
                    type: :boolean,
                    default: false,
                    doc:
                      "When true, upgrades the TCP connection with TLS via `:ssl.connect/3`. Options are passed in `:tls_opts`."
                  ],
                  tls_opts: [
                    type: :keyword_list,
                    default: [],
                    doc:
                      "Keyword list passed to `:ssl.connect/3` after TCP connect (certificates, verify modes, etc.)."
                  ],
                  defaults: [
                    type: :keyword_list,
                    keys: @defaults_keys,
                    default: [],
                    doc:
                      "Policy defaults per command (`:write`, `:read`, `:batch`, `:scan`, `:query`, …)."
                  ]
                )

  @write_schema NimbleOptions.new!(@write_keys)
  @read_schema NimbleOptions.new!(@read_keys)
  @delete_schema NimbleOptions.new!(@delete_keys)
  @exists_schema NimbleOptions.new!(@exists_keys)
  @index_create_schema NimbleOptions.new!(@index_create_keys)
  @info_schema NimbleOptions.new!(@info_keys)
  @touch_schema NimbleOptions.new!(@touch_keys)
  @operate_schema NimbleOptions.new!(@operate_keys)
  @batch_schema NimbleOptions.new!(@batch_keys)
  @udf_keys [
    timeout: [type: :non_neg_integer],
    filter: [type: {:struct, Exp}],
    pool_checkout_timeout: [type: :non_neg_integer],
    replica: [type: @replica_opt]
  ]

  @scan_schema NimbleOptions.new!(@scan_keys)
  @query_schema NimbleOptions.new!(@query_keys)
  @udf_schema NimbleOptions.new!(@udf_keys)

  @doc false
  def start_schema, do: @start_schema

  @doc false
  def validate_start(opts) when is_list(opts), do: NimbleOptions.validate(opts, @start_schema)

  @doc false
  def validate_start!(opts) when is_list(opts) do
    case validate_start(opts) do
      {:ok, validated} ->
        validated

      {:error, %NimbleOptions.ValidationError{} = e} ->
        raise ArgumentError, Exception.message(e)
    end
  end

  @doc false
  def validate_write(opts), do: NimbleOptions.validate(opts, @write_schema)

  @doc false
  def validate_read(opts) do
    case NimbleOptions.validate(opts, @read_schema) do
      {:ok, validated} -> validate_bins_not_empty_when_present(validated)
      {:error, _} = err -> err
    end
  end

  @doc false
  def validate_delete(opts), do: NimbleOptions.validate(opts, @delete_schema)

  @doc false
  def validate_exists(opts), do: NimbleOptions.validate(opts, @exists_schema)

  @doc false
  def validate_index_create(opts), do: NimbleOptions.validate(opts, @index_create_schema)

  @doc false
  def validate_info(opts), do: NimbleOptions.validate(opts, @info_schema)

  @doc false
  def validate_touch(opts), do: NimbleOptions.validate(opts, @touch_schema)

  @doc false
  def validate_operate(opts), do: NimbleOptions.validate(opts, @operate_schema)

  @doc false
  def validate_batch(opts), do: NimbleOptions.validate(opts, @batch_schema)

  @doc false
  def validate_scan(opts), do: NimbleOptions.validate(opts, @scan_schema)

  @doc false
  def validate_query(opts), do: NimbleOptions.validate(opts, @query_schema)

  @doc false
  def validate_udf(opts), do: NimbleOptions.validate(opts, @udf_schema)

  @doc false
  def validation_error_message(%NimbleOptions.ValidationError{} = e), do: Exception.message(e)

  # `:bins => []` is ambiguous (read no bins vs read all); require omission for \"all bins\".
  defp validate_bins_not_empty_when_present(validated) do
    if Keyword.get(validated, :bins) == [] do
      {:error,
       %NimbleOptions.ValidationError{
         key: :bins,
         keys_path: [],
         value: [],
         message: "must be a non-empty list of bin names; omit :bins entirely to read all bins"
       }}
    else
      {:ok, validated}
    end
  end

  # -- Wire-level policy application -------------------------------------------
  # These functions set info2/info3 flag bits and header fields on an AsmMsg
  # based on validated user options.

  @doc false
  @spec apply_write_policy(AsmMsg.t(), keyword()) :: AsmMsg.t()
  def apply_write_policy(%AsmMsg{} = msg, opts) when is_list(opts) do
    msg
    |> put_ttl(Keyword.get(opts, :ttl))
    |> put_timeout(Keyword.get(opts, :timeout))
    |> apply_generation_flags(opts)
    |> apply_exists_flags(opts)
    |> maybe_durable_delete_write(Keyword.get(opts, :durable_delete))
  end

  defp put_ttl(msg, nil), do: msg
  defp put_ttl(msg, ttl), do: %{msg | expiration: ttl}

  defp put_timeout(msg, nil), do: msg
  defp put_timeout(msg, t), do: %{msg | timeout: t}

  # Sets generation check flags. If `:generation` is given without an explicit
  # `:gen_policy`, defaults to `:expect_gen_equal` (optimistic locking).
  defp apply_generation_flags(msg, opts) do
    gen = Keyword.get(opts, :generation)

    pol =
      case Keyword.get(opts, :gen_policy) do
        nil when is_integer(gen) -> :expect_gen_equal
        nil -> :none
        other -> other
      end

    case {pol, gen} do
      {:expect_gen_equal, g} when is_integer(g) ->
        %{msg | info2: msg.info2 ||| AsmMsg.info2_generation(), generation: g}

      {:expect_gen_gt, g} when is_integer(g) ->
        %{msg | info2: msg.info2 ||| AsmMsg.info2_generation_gt(), generation: g}

      _ ->
        msg
    end
  end

  # Maps the `:exists` option to the correct info2/info3 flag bit.
  # Note: `:create_only` uses info2 while the others use info3.
  defp apply_exists_flags(msg, opts) do
    case Keyword.get(opts, :exists) do
      :create_only ->
        %{msg | info2: msg.info2 ||| AsmMsg.info2_create_only()}

      :update_only ->
        %{msg | info3: msg.info3 ||| AsmMsg.info3_update_only()}

      :replace_only ->
        %{msg | info3: msg.info3 ||| AsmMsg.info3_replace_only()}

      :create_or_replace ->
        %{msg | info3: msg.info3 ||| AsmMsg.info3_create_or_replace()}

      _ ->
        msg
    end
  end

  # Durable delete tells the server to leave a tombstone for the record.
  defp maybe_durable_delete_write(msg, true) do
    %{msg | info2: msg.info2 ||| AsmMsg.info2_durable_delete()}
  end

  defp maybe_durable_delete_write(msg, _), do: msg

  @doc false
  @spec apply_read_policy(AsmMsg.t(), keyword()) :: AsmMsg.t()
  def apply_read_policy(%AsmMsg{} = msg, opts) when is_list(opts) do
    put_timeout(msg, Keyword.get(opts, :timeout))
  end

  @doc false
  @spec apply_delete_policy(AsmMsg.t(), keyword()) :: AsmMsg.t()
  def apply_delete_policy(%AsmMsg{} = msg, opts) when is_list(opts) do
    msg
    |> put_timeout(Keyword.get(opts, :timeout))
    |> maybe_durable_delete_write(Keyword.get(opts, :durable_delete))
  end

  @doc false
  @spec apply_touch_policy(AsmMsg.t(), keyword()) :: AsmMsg.t()
  def apply_touch_policy(%AsmMsg{} = msg, opts) when is_list(opts) do
    msg
    |> put_ttl(Keyword.get(opts, :ttl))
    |> put_timeout(Keyword.get(opts, :timeout))
  end

  # Operate: timeout always applies; write-style flags apply only when the op list includes writes.
  @doc false
  @spec apply_operate_policy(AsmMsg.t(), keyword(), boolean()) :: AsmMsg.t()
  def apply_operate_policy(%AsmMsg{} = msg, opts, has_write?)
      when is_list(opts) and is_boolean(has_write?) do
    msg = put_timeout(msg, Keyword.get(opts, :timeout))

    if has_write? do
      msg
      |> put_ttl(Keyword.get(opts, :ttl))
      |> apply_generation_flags(opts)
      |> apply_exists_flags(opts)
      |> maybe_durable_delete_write(Keyword.get(opts, :durable_delete))
    else
      msg
    end
  end

  # Outer batch AS_MSG timeout (milliseconds, signed int32 on wire).
  @doc false
  @spec apply_batch_outer_timeout(AsmMsg.t(), keyword()) :: AsmMsg.t()
  def apply_batch_outer_timeout(%AsmMsg{} = msg, opts) when is_list(opts) do
    put_timeout(msg, Keyword.get(opts, :timeout))
  end

  # Appends a KEY field to the message so the server stores the original user key
  # alongside the digest. Without this, only the 20-byte digest is stored.
  @doc false
  @spec apply_send_key(AsmMsg.t(), Key.t(), keyword()) :: AsmMsg.t()
  def apply_send_key(%AsmMsg{} = msg, %Key{} = key, opts) when is_list(opts) do
    if Keyword.get(opts, :send_key) == true do
      case Field.key_from_user_key(%{user_key: key.user_key}) do
        nil -> msg
        field -> %{msg | fields: msg.fields ++ [field]}
      end
    else
      msg
    end
  end

  # Builds the read AsmMsg, varying the wire shape based on options:
  # - `header_only: true` → READ + NOBINDATA flags (metadata only, no bin payload)
  # - `bins: [...]` → READ with per-bin READ operations (selective bin fetch)
  # - otherwise → full record read (all bins)
  @doc false
  @spec read_message_for_opts(Key.t(), keyword()) :: AsmMsg.t()
  def read_message_for_opts(%Key{} = key, opts) when is_list(opts) do
    ns = key.namespace
    set = key.set
    digest = key.digest
    bins = Keyword.get(opts, :bins)

    cond do
      Keyword.get(opts, :header_only) == true ->
        %AsmMsg{
          info1: AsmMsg.info1_read() ||| AsmMsg.info1_nobindata(),
          fields: [Field.namespace(ns), Field.set(set), Field.digest(digest)]
        }

      is_list(bins) and bins != [] ->
        ops =
          bins
          |> Enum.map(&bin_name_to_string/1)
          |> Enum.sort()
          |> Enum.map(&Operation.read/1)

        %AsmMsg{
          info1: AsmMsg.info1_read(),
          fields: [Field.namespace(ns), Field.set(set), Field.digest(digest)],
          operations: ops
        }

      true ->
        AsmMsg.read_command(ns, set, digest)
    end
  end

  defp bin_name_to_string(b) when is_binary(b), do: b
  defp bin_name_to_string(a) when is_atom(a), do: Atom.to_string(a)
end
