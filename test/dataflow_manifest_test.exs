defmodule Dataflow.ManifestTest do
  use ExUnit.Case, async: false

  alias Dataflow.Manifest

  @sdk_version Dataflow.sdk_version()

  describe "build_manifest/3" do
    test "pure map shape with explicitly passed dependencies" do
      deps = [
        %{"name" => "ecto", "version" => "3.13.2"},
        %{"name" => "jason", "version" => "1.4.4"}
      ]

      manifest = Manifest.build_manifest("payments", @sdk_version, deps)

      assert %{
               "service_name" => "payments",
               "language" => "elixir",
               "sdk_version" => @sdk_version,
               "runtime_version" => runtime_version,
               "framework" => "",
               "os_arch" => os_arch,
               "dependencies" => ^deps
             } = manifest

      assert is_binary(runtime_version) and runtime_version != ""
      assert is_binary(os_arch) and String.contains?(os_arch, "/")
    end

    test "first known framework match wins (phoenix precedes cowboy)" do
      deps = [
        %{"name" => "cowboy", "version" => "2.13.0"},
        %{"name" => "phoenix", "version" => "1.7.21"}
      ]

      assert Manifest.build_manifest("svc", @sdk_version, deps)["framework"] == "phoenix"
    end

    test "plug-only dependency set matches plug" do
      deps = [%{"name" => "plug", "version" => "1.18.1"}]
      assert Manifest.build_manifest("svc", @sdk_version, deps)["framework"] == "plug"
    end

    test "unknown dependency set reports empty framework" do
      deps = [%{"name" => "jason", "version" => "1.4.4"}]
      assert Manifest.build_manifest("svc", @sdk_version, deps)["framework"] == ""
    end

    test "empty dependency set reports empty framework and empty deps" do
      manifest = Manifest.build_manifest("svc", @sdk_version, [])
      assert manifest["framework"] == ""
      assert manifest["dependencies"] == []
    end

    test "app_version reads DATAFLOW_APP_VERSION" do
      System.put_env("DATAFLOW_APP_VERSION", "9.9.9-rc1")
      on_exit(fn -> System.delete_env("DATAFLOW_APP_VERSION") end)

      manifest = Manifest.build_manifest("svc", @sdk_version, [])
      assert manifest["app_version"] == "9.9.9-rc1"
    end

    test "unset DATAFLOW_APP_VERSION yields empty app_version" do
      System.delete_env("DATAFLOW_APP_VERSION")
      manifest = Manifest.build_manifest("svc", @sdk_version, [])
      assert manifest["app_version"] == ""
    end
  end

  describe "http_base_url/1" do
    test "DATAFLOW_HTTP_URL override wins, whitespace and trailing slash trimmed" do
      System.put_env("DATAFLOW_HTTP_URL", " http://api:8080/ ")
      on_exit(fn -> System.delete_env("DATAFLOW_HTTP_URL") end)

      assert Manifest.http_base_url("api:9090") == "http://api:8080"
    end

    test "url-form endpoint used as-is (scheme preserved), trailing slash trimmed" do
      System.delete_env("DATAFLOW_HTTP_URL")

      assert Manifest.http_base_url("https://ingest.example.com") == "https://ingest.example.com"
      assert Manifest.http_base_url("http://ingest.example.com/") == "http://ingest.example.com"
    end

    test "bare gRPC host:port has no derivable HTTP base" do
      System.delete_env("DATAFLOW_HTTP_URL")

      assert Manifest.http_base_url("api:9090") == nil
    end
  end

  describe "loaded_deps/0" do
    test "running apps as name/version pairs, sorted, dataflow excluded, capped" do
      deps = Manifest.loaded_deps()

      assert is_list(deps)
      assert length(deps) <= 500

      names = Enum.map(deps, & &1["name"])
      assert names == Enum.sort(names)
      assert "dataflow" not in names
      # Always-running BEAM applications under `mix test`.
      assert "kernel" in names and "stdlib" in names

      Enum.each(deps, fn dep ->
        assert is_binary(dep["name"]) and is_binary(dep["version"])
      end)
    end
  end

  describe "send_manifest/0" do
    test "no-op without a derivable HTTP base or API key" do
      Application.put_env(:dataflow, :settings, %{
        endpoint: "api:9090",
        api_key: "",
        service_name: "svc",
        sample_ratio: 1.0,
        buffer_size: 10_000,
        disabled: false
      })

      on_exit(fn -> Application.delete_env(:dataflow, :settings) end)
      System.delete_env("DATAFLOW_HTTP_URL")

      assert Manifest.send_manifest() == :ok
    end
  end
end
