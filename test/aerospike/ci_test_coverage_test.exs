defmodule Aerospike.CITestCoverageTest do
  use ExUnit.Case, async: true

  @repo_root Path.expand("../..", __DIR__)
  @workflow Path.join(@repo_root, ".github/workflows/integration-ce.yml")

  test "integration aliases enumerate every integration test file by tag" do
    assert alias_files("test.integration.ce") == integration_files(:ce)
    assert alias_files("test.integration.cluster") == integration_files(:cluster)
    assert alias_files("test.integration.enterprise") == integration_files(:enterprise)
  end

  test "GitHub Actions run every test family" do
    ci = File.read!(Path.join(@repo_root, ".github/workflows/ci.yml"))
    integration = File.read!(@workflow)

    assert ci =~ "mix validate"
    assert integration =~ "mix test.integration.ce"
    assert integration =~ "mix test.integration.cluster"
    assert integration =~ "mix test.integration.enterprise"
  end

  defp alias_files(name) do
    Mix.Project.config()
    |> Keyword.fetch!(:aliases)
    |> Keyword.fetch!(String.to_atom(name))
    |> List.wrap()
    |> Enum.flat_map(&extract_test_files/1)
    |> Enum.sort()
  end

  defp extract_test_files(command) when is_binary(command) do
    ~r/test\/integration\/[^ ]+_test\.exs/
    |> Regex.scan(command)
    |> List.flatten()
  end

  defp extract_test_files(_alias_or_fun), do: []

  defp integration_files(kind) do
    @repo_root
    |> Path.join("test/integration/*_test.exs")
    |> Path.wildcard()
    |> Enum.filter(&matches_kind?(&1, kind))
    |> Enum.map(&Path.relative_to(&1, @repo_root))
    |> Enum.sort()
  end

  defp matches_kind?(path, :ce) do
    content = File.read!(path)

    String.contains?(content, "@moduletag :integration") and
      not String.contains?(content, "@moduletag :cluster") and
      not String.contains?(content, "@moduletag :enterprise")
  end

  defp matches_kind?(path, :cluster) do
    path
    |> File.read!()
    |> String.contains?("@moduletag :cluster")
  end

  defp matches_kind?(path, :enterprise) do
    path
    |> File.read!()
    |> String.contains?("@moduletag :enterprise")
  end
end
