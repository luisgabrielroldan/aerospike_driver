defmodule Aerospike.RepositoryBoundaryTest do
  use ExUnit.Case, async: true

  @repo_root Path.expand("../..", __DIR__)

  @forbidden_fragments [
    "spike" <> "-docs",
    "aerospike_driver" <> "_old",
    "aerospike_driver" <> "_spike",
    "official" <> "_libs",
    "inspirational" <> "_libs",
    "MEMORY" <> ".md",
    "docs/" <> "old-plans",
    "docs/" <> "plans",
    "from the " <> "workspace",
    "workspace" <> " root",
    "dev " <> "workspace",
    "outer " <> "root",
    "/Users/" <> "groldan/Dev/Elixir/aerospike",
    "/home/runner/work/aerospike_driver/" <> "spike" <> "-" <> "docs"
  ]

  @scanned_extensions ~w[
    .ex .exs .md .yml .yaml .json .txt .formatter .lock .make .sh
  ]

  test "tracked repository files do not reference external development artifacts" do
    assert forbidden_references() == []
  end

  defp forbidden_references do
    for file <- tracked_files(),
        scanned_file?(file),
        content = File.read!(Path.join(@repo_root, file)),
        fragment <- @forbidden_fragments,
        String.contains?(content, fragment) do
      {file, fragment}
    end
  end

  defp tracked_files do
    case System.cmd("git", ["ls-files"], cd: @repo_root, stderr_to_stdout: true) do
      {files, 0} -> String.split(files, "\n", trim: true)
      {error, _status} -> flunk("git ls-files failed: #{error}")
    end
  end

  defp scanned_file?(file) do
    extension = Path.extname(file)
    extension in @scanned_extensions or file in ["Makefile", "README.md"]
  end
end
