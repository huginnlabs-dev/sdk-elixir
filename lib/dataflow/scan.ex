defmodule Dataflow.Scan do
  @moduledoc """
  Static route scanner: extracts the HTTP endpoints a Phoenix or Plug
  router declares from the project's `*.ex`/`*.exs` sources and posts them
  to the server's route catalog (`POST /api/v1/catalog`). Meant to run once
  per release, from any project:

      mix run -e "Dataflow.Scan.run()"

  Extraction is line/regex based (no AST dependency) and the router is the
  source of truth — controller-side `def show(conn, _params)` handling is
  deliberately skipped. Recognized shapes:

    * Phoenix routers — a `use MyAppWeb, :router` or `scope "..."` line —
      with `get "/orders/:id", OrderController, :show` routes; `scope`
      path prefixes are applied (nested scopes concatenate) and
      get/post/put/patch/delete/options/trace are upcased.
    * Plug routers — `use Plug.Router` — with `get "/path" do` match
      blocks, reported with an empty handler (the do-block has no named
      handler).

  Best-effort like the manifest: without a derivable HTTP base or API key
  the run is skipped, and every failure returns `{:error, reason}` instead
  of raising into the caller.
  """

  require Logger

  @max_routes 1000
  @timeout_ms 5_000
  @skip_dirs ~w(deps _build .git test)

  # Phoenix router markers: `use MyAppWeb, :router` ...
  @use_router_re ~r/^\s*use\s+[A-Z][\w.]*\s*,\s*:router\b/m
  # ... or any `scope "..."` opener (routers that skip the use line).
  @scope_re ~r/^\s*scope\s+"([^"]*)"/
  # `get "/orders/:id", OrderController, :show`
  @phoenix_route_re ~r/^\s*(get|post|put|patch|delete|options|trace)\s+"([^"]*)"\s*,\s*([A-Z][\w.]*)\s*,\s*:(\w+)/
  # Plain Plug routers: `use Plug.Router` with `get "/path" do` blocks.
  @plug_router_re ~r/^\s*use\s+Plug\.Router\b/m
  @plug_route_re ~r/^\s*(get|post|put|patch|delete|options)\s+"([^"]*)"\s+do\b/

  @doc """
  Scans `opts[:dir]` for declared routes and reports the catalog.
  Options:

    * `:dir` — project root to scan (default `"."`)
    * `:service` — service label (default `DATAFLOW_SERVICE_NAME`, then
      the directory's basename)
    * `:url` — explicit base URL override (default: `DATAFLOW_HTTP_URL`,
      else the URL-form `DATAFLOW_ENDPOINT`, resolved exactly like the
      manifest)
    * `:print` — print the catalog JSON instead of posting

  Returns `{:ok, route_count}`, `{:error, reason}` or `:skipped`; never
  raises into the caller.
  """
  def run(opts \\ []) when is_list(opts) do
    dir = Keyword.get(opts, :dir, ".")

    service =
      Keyword.get(opts, :service) || non_empty_env("DATAFLOW_SERVICE_NAME") || default_service(dir)

    routes = dir |> collect_sources() |> extract() |> Enum.take(@max_routes)
    body = catalog(service, routes)

    if Keyword.get(opts, :print, false) do
      IO.puts(JSON.encode!(body))
      {:ok, length(routes)}
    else
      report(Keyword.get(opts, :url), body)
    end
  rescue
    e -> {:error, Exception.message(e) || inspect(e)}
  catch
    _kind, value -> {:error, inspect(value)}
  end

  @doc "Builds the exact catalog POST body map for a service and its routes."
  def catalog(service_name, routes) when is_list(routes) do
    %{"service_name" => to_string(service_name), "routes" => routes}
  end

  @doc """
  Pure extraction: takes a list of `{filename, source}` pairs and returns
  the declared routes in file order — `%{"method" => ..., "path" => ...,
  "handler" => ..., "source_file" => ...}` maps. Non-router files yield [].
  """
  def extract(files) when is_list(files) do
    Enum.flat_map(files, &extract_file/1)
  end

  def extract(_files), do: []

  # --- per-file extraction -----------------------------------------------------

  defp extract_file({filename, source}) when is_binary(source) do
    cond do
      Regex.match?(@use_router_re, source) or Regex.match?(@scope_re, source) ->
        phoenix_routes(filename, source)

      Regex.match?(@plug_router_re, source) ->
        plug_routes(filename, source)

      true ->
        []
    end
  end

  defp extract_file(_entry), do: []

  # Line walker with conservative do/end tracking: scope openers push their
  # path prefix (with the depth they opened at), a matching `end` pops it,
  # and every other `do`-terminated line just moves the depth. Route lines
  # are matched against the concatenated scope prefixes.
  defp phoenix_routes(filename, source) do
    {routes, _scopes, _depth} =
      source
      |> String.split(["\r\n", "\n"])
      |> Enum.reduce({[], [], 0}, fn line, acc -> phoenix_line(line, filename, acc) end)

    Enum.reverse(routes)
  end

  defp phoenix_line(line, filename, {routes, scopes, depth} = acc) do
    trimmed = String.trim(line)

    cond do
      scope_open?(line, trimmed) ->
        [_, raw] = Regex.run(@scope_re, line)
        {routes, [{scope_prefix(raw), depth} | scopes], depth + 1}

      trimmed == "end" ->
        case scopes do
          [{_path, opened} | rest] when opened == depth - 1 -> {routes, rest, depth - 1}
          _ -> {routes, scopes, max(depth - 1, 0)}
        end

      match = Regex.run(@phoenix_route_re, line) ->
        route = phoenix_route(filename, match, join_prefixes(scopes))
        {[route | routes], scopes, depth}

      String.ends_with?(trimmed, " do") or trimmed == "do" ->
        {routes, scopes, depth + 1}

      true ->
        acc
    end
  end

  defp scope_open?(line, trimmed) do
    String.ends_with?(trimmed, "do") and Regex.match?(@scope_re, line)
  end

  defp phoenix_route(filename, [_, method, raw_path, controller, action], prefix) do
    %{
      "method" => String.upcase(method),
      "path" => join_path(prefix, raw_path),
      "handler" => controller <> "." <> action,
      "source_file" => filename
    }
  end

  defp plug_routes(filename, source) do
    source
    |> String.split(["\r\n", "\n"])
    |> Enum.flat_map(fn line ->
      case Regex.run(@plug_route_re, line) do
        [_, method, raw_path] ->
          [
            %{
              "method" => String.upcase(method),
              "path" => join_path("", raw_path),
              "handler" => "",
              "source_file" => filename
            }
          ]

        nil ->
          []
      end
    end)
  end

  # Outermost first, concatenated ("/api" + "/v1" -> "/api/v1").
  defp join_prefixes(scopes) do
    scopes
    |> Enum.reverse()
    |> Enum.map(&elem(&1, 0))
    |> Enum.join("")
  end

  # "/" -> "" (joins invisibly), "api" -> "/api", "/api/" -> "/api".
  defp scope_prefix(raw) do
    case String.trim(raw) do
      "" ->
        ""

      path ->
        path = "/" <> String.trim_leading(path, "/")
        String.trim_trailing(path, "/")
    end
  end

  # Paths always start with "/" (server contract), scope prefix or not.
  defp join_path(prefix, raw_path) do
    path = String.trim(raw_path)

    if String.starts_with?(path, "/") do
      prefix <> path
    else
      prefix <> "/" <> path
    end
  end

  # --- source collection ---------------------------------------------------------

  defp collect_sources(dir) do
    root = Path.expand(dir)
    paths = Path.wildcard(Path.join(root, "**/*.ex")) ++ Path.wildcard(Path.join(root, "**/*.exs"))

    paths
    |> Enum.reject(&skip_path?/1)
    |> Enum.sort()
    |> Enum.flat_map(fn path ->
      case File.read(path) do
        {:ok, source} ->
          [{relative(path, root), source}]

        {:error, reason} ->
          Logger.debug("dataflow: scan skipping #{path}: #{inspect(reason)}")
          []
      end
    end)
  rescue
    e ->
      Logger.debug("dataflow: scan could not list #{dir}: #{Exception.message(e)}")
      []
  end

  defp skip_path?(path) do
    path |> Path.split() |> Enum.any?(&(&1 in @skip_dirs))
  end

  defp relative(path, root) do
    path |> Path.relative_to(root) |> String.replace("\\", "/")
  end

  # --- reporting -------------------------------------------------------------------

  # Base URL resolution mirrors Dataflow.Manifest.http_base_url/1; :url wins.
  defp report(url_override, body) do
    base =
      case url_override do
        url when is_binary(url) and url != "" -> String.trim_trailing(url, "/")
        _ -> Dataflow.Manifest.http_base_url(settings(:endpoint, ""))
      end

    api_key = settings(:api_key, "")

    cond do
      not is_binary(base) or base == "" ->
        Logger.debug("dataflow: scan skipped - no derivable HTTP base URL")
        :skipped

      api_key == "" ->
        Logger.debug("dataflow: scan skipped - no API key configured")
        :skipped

      true ->
        post_catalog(base, api_key, body)
    end
  end

  defp post_catalog(base, api_key, body) do
    json = JSON.encode!(body)
    headers = [{~c"content-type", ~c"application/json"}, {~c"x-api-key", String.to_charlist(api_key)}]
    request = {String.to_charlist(base <> "/api/v1/catalog"), headers, ~c"application/json", json}
    count = length(body["routes"])

    case :httpc.request(:post, request, [timeout: @timeout_ms, connect_timeout: @timeout_ms], [body_format: :binary]) do
      {:ok, {{_http_version, 200, _reason}, _headers, _resp_body}} ->
        {:ok, count}

      {:ok, {{_http_version, status, reason}, _headers, _resp_body}} ->
        {:error, "catalog response ignored (#{status} #{reason})"}

      {:error, reason} ->
        {:error, "catalog send failed: #{inspect(reason)}"}
    end
  rescue
    e -> {:error, Exception.message(e) || inspect(e)}
  catch
    _kind, value -> {:error, inspect(value)}
  end

  # --- helpers -----------------------------------------------------------------------

  defp default_service(dir) do
    dir |> Path.expand() |> Path.basename()
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
