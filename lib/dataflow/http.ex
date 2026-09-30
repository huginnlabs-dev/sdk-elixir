defmodule Dataflow.HTTP do
  @moduledoc """
  Traced outgoing HTTP: a thin wrapper around Erlang's built-in `:httpc`
  that turns every call into an HTTP_CLIENT span (service-boundary
  tracing) and propagates the trace id via the `X-Dataflow-Trace-Id`
  header, so an instrumented receiver joins the same trace.

  Best-effort by design: with tracing disabled the wrapper is a plain
  pass-through, and span bookkeeping never alters the request outcome.

      {:ok, 200, _headers, body} =
        Dataflow.HTTP.post("https://api.example.com/v1/events",
                           [{"x-api-key", key}], json)

  Returns `{:ok, status, headers, body}` with headers and body exactly as
  `:httpc` delivers them, or `{:error, reason}`. Options:

    * `:timeout` — request timeout in ms (default 30_000)
    * `:connect_timeout` — connect timeout in ms (default 8_000)
    * `:content_type` — request content type for body-ful methods
      (default "application/json")
  """

  @trace_header "x-dataflow-trace-id"

  @default_timeout 30_000
  @default_connect_timeout 8_000
  @default_content_type "application/json"

  @bodyful ~w(post put patch)a

  @doc """
  Performs an HTTP request and emits the surrounding HTTP_CLIENT span —
  name "METHOD host/path", host as the callee package, HTTP status as the
  span status. `method` is an atom (:get, :post, :put, :patch, :delete,
  :head, :options); `headers` is a list of `{name, value}` pairs (values
  are stringified); `body` is a binary (empty for body-less methods).
  """
  def request(method, url, headers \\ [], body \\ "", opts \\ [])
      when is_atom(method) and is_binary(url) and is_list(headers) and is_binary(body) and is_list(opts) do
    if Dataflow.enabled?() do
      traced_request(method, url, headers, body, opts)
    else
      raw_request(method, url, headers, body, opts)
    end
  end

  @doc "Convenience GET returning `request(:get, url, headers)`."
  def get(url, headers \\ []) when is_binary(url) and is_list(headers),
    do: request(:get, url, headers)

  @doc "Convenience POST returning `request(:post, url, headers, body)`."
  def post(url, headers \\ [], body \\ "") when is_binary(url) and is_list(headers) and is_binary(body),
    do: request(:post, url, headers, body)

  # --- traced path -----------------------------------------------------------

  defp traced_request(method, url, headers, body, opts) do
    span =
      Dataflow.start_span(span_name(method, url), "HTTP_CLIENT")
      |> Dataflow.Span.callee(host(url))
      |> Dataflow.Span.attr("http.method", String.upcase(to_string(method)))
      |> Dataflow.Span.attr("http.url", url)

    headers =
      [{@trace_header, span.trace_id} | headers]
      |> Enum.map(fn {k, v} -> {String.downcase(to_string(k)), String.to_charlist(to_string(v))} end)

    case raw_request_with_headers(method, url, headers, body, opts) do
      {:ok, status, resp_headers, resp_body} ->
        span |> Dataflow.Span.status(status) |> Dataflow.Span.end_span()
        {:ok, status, resp_headers, resp_body}

      {:error, reason} ->
        span
        |> Dataflow.Span.record_error(reason_text(reason))
        |> Dataflow.Span.end_span()

        {:error, reason}
    end
  rescue
    e -> {:error, Exception.message(e)}
  end

  # --- plain path ------------------------------------------------------------

  defp raw_request(method, url, headers, body, opts) do
    headers = Enum.map(headers, fn {k, v} -> {String.downcase(to_string(k)), String.to_charlist(to_string(v))} end)
    raw_request_with_headers(method, url, headers, body, opts)
  end

  defp raw_request_with_headers(method, url, headers, body, opts) do
    http_opts = [
      timeout: opt(opts, :timeout, @default_timeout),
      connect_timeout: opt(opts, :connect_timeout, @default_connect_timeout)
    ]

    request =
      if body == "" and method not in @bodyful do
        {String.to_charlist(url), headers}
      else
        {String.to_charlist(url), headers, String.to_charlist(to_string(opt(opts, :content_type, @default_content_type))), body}
      end

    case :httpc.request(method, request, http_opts, body_format: :binary) do
      {:ok, {{_http_version, status, _reason_phrase}, resp_headers, resp_body}} ->
        {:ok, status, resp_headers, resp_body}

      {:error, reason} ->
        {:error, reason}
    end
  end

  # --- span derivation (pure, unit-tested) ------------------------------------

  @doc false
  # "GET api.example.com/v1/orders" — method, host and path, like the Go SDK.
  def span_name(method, url) do
    uri = URI.parse(url)
    "#{String.upcase(to_string(method))} #{uri.host || ""}#{uri.path || ""}"
  end

  @doc false
  # Authority part of the URL (host, with the port when explicitly present)
  # — the span's callee_package. Unparseable URLs yield "".
  def host(url) do
    case URI.parse(url) do
      %URI{host: nil} -> ""
      %URI{host: host, port: nil} -> host
      %URI{host: host, port: port} -> "#{host}:#{port}"
    end
  end

  defp opt(opts, key, fallback) do
    case Keyword.fetch(opts, key) do
      {:ok, value} -> value
      :error -> fallback
    end
  end

  defp reason_text(reason) when is_binary(reason), do: reason
  defp reason_text(reason), do: inspect(reason)
end
