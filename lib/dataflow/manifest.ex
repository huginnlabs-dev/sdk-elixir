defmodule Dataflow.Manifest do
  @moduledoc """
  Service manifest: one best-effort HTTP POST at startup describing this
  service (framework, runtime, dependency inventory from the running
  applications). The server turns it into the project's service catalog.
  Failures are silent — tracing never depends on the manifest reaching
  the server.
  """

  require Logger

  @max_deps 500
  @timeout_ms 5_000

  # First match wins (order matters); everything else reports as "".
  @known_frameworks [
    {"phoenix", "phoenix"},
    {"plug", "plug"},
    {"bandit", "bandit"},
    {"cowboy", "cowboy"}
  ]

  @doc """
  Builds the manifest body map. `deps` defaults to the node's running
  applications; pass a list of %{"name" => _, "version" => _} explicitly
  (tests, custom inventory) to keep this pure.
  """
  def build_manifest(service_name, sdk_version, deps \\ loaded_deps()) when is_list(deps) do
    %{
      "service_name" => to_string(service_name),
      "language" => "elixir",
      "sdk_version" => sdk_version,
      "runtime_version" => runtime_version(),
      "framework" => framework_for(deps),
      "os_arch" => os_arch(),
      "app_version" => System.get_env("DATAFLOW_APP_VERSION") || "",
      "dependencies" => deps
    }
  end

  @doc false
  # Running applications as name/version pairs, sorted, capped, with the
  # SDK app itself excluded. Best-effort: any failure yields [].
  def loaded_deps do
    :application.which_applications()
    |> Enum.map(fn {app, _description, version} ->
      %{"name" => to_string(app), "version" => to_string(version)}
    end)
    |> Enum.reject(&(&1["name"] == "dataflow"))
    |> Enum.sort_by(& &1["name"])
    |> Enum.take(@max_deps)
  rescue
    _ -> []
  end

  @doc false
  def framework_for(deps) when is_list(deps) do
    names = MapSet.new(deps, & &1["name"])

    Enum.find_value(@known_frameworks, "", fn {app, framework} ->
      if MapSet.member?(names, app), do: framework
    end)
  end

  @doc false
  # HTTP API base for manifest reporting: an explicit DATAFLOW_HTTP_URL
  # wins (needed when DATAFLOW_ENDPOINT is a bare gRPC host:port); URL-form
  # endpoints map directly; anything else has no derivable HTTP base and
  # reporting is skipped.
  def http_base_url(endpoint) do
    case non_empty_env("DATAFLOW_HTTP_URL") do
      nil ->
        if String.starts_with?(endpoint, ["http://", "https://"]) do
          String.trim_trailing(endpoint, "/")
        else
          nil
        end

      override ->
        String.trim_trailing(override, "/")
    end
  end

  @doc """
  Reports the manifest once per node. Best-effort: runs on a detached task
  with a short timeout and swallows every failure, so it never blocks
  startup or tracing. No-op without a derivable HTTP base or API key.
  """
  def send_manifest do
    base = http_base_url(settings(:endpoint, ""))
    api_key = settings(:api_key, "")

    if is_binary(base) and api_key != "" do
      {:ok, pid} = Task.start(fn -> report(base, api_key) end)
      Process.unlink(pid)
    end

    :ok
  rescue
    _ -> :ok
  end

  defp report(base, api_key) do
    body = JSON.encode!(build_manifest(Dataflow.service_name(), Dataflow.sdk_version()))

    headers = [{~c"content-type", ~c"application/json"}, {~c"x-api-key", String.to_charlist(api_key)}]
    request = {String.to_charlist(base <> "/api/v1/manifest"), headers, ~c"application/json", body}

    case :httpc.request(:post, request, [timeout: @timeout_ms, connect_timeout: @timeout_ms], [body_format: :binary]) do
      {:ok, {{_http_version, 200, _reason}, _headers, _resp_body}} ->
        :ok

      {:ok, {{_http_version, status, reason}, _headers, _resp_body}} ->
        Logger.debug("dataflow: manifest response ignored (#{status} #{reason})")
        :ok

      {:error, reason} ->
        Logger.debug("dataflow: manifest send failed: #{inspect(reason)}")
        :ok
    end
  rescue
    _ -> :ok
  catch
    _kind, _value -> :ok
  end

  defp runtime_version do
    System.version()
  rescue
    _ -> ""
  end

  defp os_arch do
    {_, os} = :os.type()

    arch =
      :erlang.system_info(:system_architecture)
      |> to_string()
      |> String.split("-")
      |> hd()

    "#{os}/#{arch}"
  rescue
    _ -> ""
  end

  defp non_empty_env(key) do
    case System.get_env(key) do
      v when is_binary(v) and v != "" -> String.trim(v)
      _ -> nil
    end
  end

  defp settings(key, fallback) do
    Application.get_env(:dataflow, :settings, %{}) |> Map.get(key, fallback)
  end
end
