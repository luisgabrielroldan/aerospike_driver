defmodule Aerospike.ExpressionIndexTest do
  use ExUnit.Case, async: true

  alias Aerospike.Error
  alias Aerospike.Exp

  describe "create_expression_index/5" do
    test "rejects missing index name before command execution" do
      assert {:error, %Error{code: :invalid_argument, message: message}} =
               Aerospike.create_expression_index(:cluster, "test", "users", Exp.int_bin("age"),
                 type: :numeric
               )

      assert message =~ "expression-backed indexes require option :name"
    end

    test "rejects invalid index type before command execution" do
      assert {:error, %Error{code: :invalid_argument, message: message}} =
               Aerospike.create_expression_index(:cluster, "test", "users", Exp.int_bin("age"),
                 name: "age_expr_idx",
                 type: :boolean
               )

      assert message =~ "expression-backed index type"
      assert message =~ ":boolean"
    end

    test "rejects bin source options before command execution" do
      assert {:error, %Error{code: :invalid_argument, message: message}} =
               Aerospike.create_expression_index(:cluster, "test", "users", Exp.int_bin("age"),
                 bin: "age",
                 name: "age_expr_idx",
                 type: :numeric
               )

      assert message =~ "expression-backed indexes support only"
      assert message =~ ":bin"
    end

    test "rejects empty expression wires before command execution" do
      assert {:error, %Error{code: :invalid_argument, message: message}} =
               Aerospike.create_expression_index(:cluster, "test", "users", Exp.from_wire(""),
                 name: "age_expr_idx",
                 type: :numeric
               )

      assert message =~ "%Aerospike.Exp{}"
      assert message =~ "non-empty wire bytes"
    end
  end
end
