# Dataflow Elixir SDK

HuginnLabs Dataflow tracing for Elixir/Erlang services: runtime spans with
E2E-encrypted payloads, shipped over the REST ingest API. Spans ride the
process dictionary, so nested `Dataflow.trace/2` calls join the enclosing
trace on the BEAM; a background pipeline batches completed events and
delivers them with ack-based replay.

Requires Elixir ~> 1.18. The only hex dependency is `:telemetry` (for the
Ecto and Oban tracers); HTTP uses OTP's built-in `:httpc`, crypto uses `:crypto`,
JSON is Elixir 1.18+'s built-in.

## Setup

```elixir
# mix.exs
defp deps do
  [{:dataflow, path: "…/sdk-elixir"}]
end
```

`Dataflow.configure/0` (run automatically at application start) reads:

| Env var                 | Meaning                                          | Default     |
|-------------------------|--------------------------------------------------|-------------|
| `DATAFLOW_ENDPOINT`     | REST ingest base URL                             | —           |
| `DATAFLOW_API_KEY`      | `x-api-key` for ingest                           | —           |
| `DATAFLOW_SERVICE_NAME` | service label                                    | hostname    |
| `DATAFLOW_SAMPLE_RATIO` | 0.0–1.0                                          | `1.0`       |
| `DATAFLOW_BUFFER_SIZE`  | replay buffer cap                                | `10_000`    |
| `DATAFLOW_DISABLED`     | `"true"` turns all tracing into a no-op          | `"false"`   |
| `DATAFLOW_ENCRYPTION_KEY` | AES-256-GCM payload key (plaintext when unset) | —           |

Tracing is enabled only when endpoint and API key are set and
`DATAFLOW_DISABLED != "true"`. Every integration below degrades to a plain
pass-through otherwise.

## Manual spans

```elixir
Dataflow.trace("ingest.Validate", fn span ->
  Dataflow.Span.data(span, "event_id", ev.id)      # encrypted payload field
  Dataflow.trace("schema.Check", fn _s -> ... end) # nested → same trace
end)

# Entry points: adopt an incoming X-Dataflow-Trace-Id (Phoenix plug, …)
Dataflow.start_server_span(route, incoming_trace_id)
# … handler work …
Dataflow.end_current_span()
```

## Crash capture

`Dataflow.Crash.capture/1` wraps any zero-arity function: when the function
raises, throws or exits, the crash is recorded on the current span — status
500, the `Exception.format/3` rendering truncated to 500 chars as the error
message, and the formatted stacktrace (capped at 8192 bytes, from the top)
as the `error.stack` metadata entry. The crash is never swallowed: it
propagates exactly as it would without the SDK (re-raised with its original
stacktrace).

```elixir
Dataflow.trace("job.Run", fn _span ->
  Dataflow.Crash.capture(fn -> risky_work() end)
end)

# Top-level convenience (delegates to Dataflow.Crash.capture/1):
Dataflow.capture(fn -> risky_work() end)
```

Without a current span the crash is recorded on a synthetic `exception`
span, so crashes stay visible outside `Dataflow.trace/2`. Recording is
best-effort (it can never mask the crash) and with `DATAFLOW_DISABLED=true`
the wrapper disappears entirely: the function runs bare.

For Plug routers, `use Dataflow.PlugCrash` (after `use Plug.Router`) wraps
dispatch the same way: any handler crash is recorded on the request's span
— plus the request line as `error.request` metadata — and re-raised, so
Plug's normal error handling proceeds untouched.

```elixir
defmodule MyApp.Router do
  use Plug.Router
  use Dataflow.PlugCrash

  plug :match
  plug :dispatch
  get "/orders" do
    send_resp(conn, 200, "[]")
  end
end
```

## Outgoing HTTP tracing

`Dataflow.HTTP` is a thin wrapper around OTP's `:httpc` that turns every
call into an `HTTP_CLIENT` span — `METHOD host/path` as the name, the
host as the callee package, the HTTP status as the span status — and
injects the `X-Dataflow-Trace-Id` header, so an instrumented receiver
joins the same trace.

```elixir
{:ok, 200, headers, body} = Dataflow.HTTP.get(url)
{:ok, 200, _, body} = Dataflow.HTTP.post(url, [{"content-type", "application/json"}], json)

# Full form: request(method, url, headers, body, opts)
# opts: :timeout, :connect_timeout (ms) and :content_type
{:ok, 200, _, body} =
  Dataflow.HTTP.request(:get, url, [], "", timeout: 5_000, connect_timeout: 2_000)
```

Returns `{:ok, status, headers, body}` with headers and body exactly as
`:httpc` delivers them, or `{:error, reason}`. Failures mark the span
with the error; span bookkeeping never alters the request outcome.

## Ecto query tracing

Attach once at boot (application start / release hook):

```elixir
Dataflow.attach_ecto()                 # listens on [:dataflow_sample, :repo, :query]
Dataflow.attach_ecto([:my, :prefix])   # match your repo's telemetry prefix
Dataflow.detach_ecto()                 # when needed
```

Every `[:<prefix>, :repo, :query]` telemetry event becomes a `DB_QUERY`
span:

- name `VERB table` derived from the SQL (`"SELECT orders"`,
  `"INSERT users"`, `IF [NOT] EXISTS` skipped, `public.items` → `items`);
- the database system (`postgres`, `mysql`, `sqlite`, …) derived from the
  repo's adapter as the callee package;
- `db.system` and the single-spaced, 200-char-truncated `db.statement`
  in metadata — parameter values are never captured;
- duration from the telemetry measurements;
- failures marked status 500 with the error message.

Spans join the caller's current trace when one exists (e.g. a Phoenix
request span), otherwise they open their own. The handler is best-effort:
it never raises into the query caller.

## Oban jobs

Attach once at boot (application start / release hook):

```elixir
Dataflow.attach_oban()                  # listens on [:oban, :job, :start/:stop/:exception]
Dataflow.attach_oban([:my, :prefix])    # match your Oban telemetry prefix
Dataflow.detach_oban()                  # when needed
```

Every Oban job lifecycle becomes one `FUNCTION_CALL` span (the `:start`
event opens it, the `:stop`/`:exception` event closes it):

- name `oban.<worker>` — the worker module from the job map;
- `oban.queue` and `oban.attempt` in metadata — job args are never
  captured (they may carry end-user data);
- duration from the stop/exception measurements;
- failures (the `:exception` event) marked status 500 with the formatted
  error (truncated to 500 chars) and the stacktrace capped at 8192 bytes
  as the `error.stack` metadata entry.

Spans join the caller's current trace when one exists, otherwise they
open their own. The handler is best-effort: it never raises into the job
process, and with `DATAFLOW_DISABLED=true` it is a no-op.

## Log capture

Application logs ship through the same configuration as spans, correlated
with the current trace:

```elixir
Dataflow.info("cache warmed", %{"entries" => n}) # level, message, fields
Dataflow.debug("detail")                         # debug/info/warn/error
Dataflow.log("warn", "disk 90%", %{"pct" => 90})

# Forward Elixir Logger messages into the same pipeline:
Dataflow.attach_logger()                         # install once at boot
Dataflow.detach_logger()                         # when needed
```

Every line becomes a wire entry of `timestamp` (unix ms), `level`,
`message`, `trace_id`, `span_id`, `service_name` and `fields`. The
trace/span ids come from the caller's current span (`Dataflow.trace/2`,
`start_server_span/2`), so logs land on the trace that produced them.
Fields — and Logger metadata, for the handler — are stringified and
capped at 50 entries; levels normalize to `debug`/`info`/`warn`/`error`
(Logger's `warning` maps to `warn`, `critical`/`alert`/`emergency` to
`error`).

Delivery is batched and best-effort: a background flusher posts to
`POST {base}/api/v1/logs` every 500 ms, or as soon as 50 lines are
buffered — at most 1000 entries per batch, `x-api-key` auth, 5 s timeout,
one retry then drop. The buffer holds 1024 entries and drops the oldest on
overflow. The base URL resolves like the manifest's (`DATAFLOW_HTTP_URL`,
else the URL-form `DATAFLOW_ENDPOINT`); a bare `host:port` endpoint has no
derivable HTTP base and log shipping stays off. With
`DATAFLOW_DISABLED=true` every entry point is a no-op, and logging never
raises into the caller.

## Route scanning

`Dataflow.Scan` is a static scanner for CI / release pipelines: it extracts
the HTTP endpoints a Phoenix or Plug router declares (line/regex based, no
AST) and posts them to the server's route catalog, so dashboards show every
declared route before the first trace arrives.

```sh
mix run -e "Dataflow.Scan.run()"
```

- Scans `*.ex`/`*.exs` under the current directory (skipping `deps/`,
  `_build/`, `.git/` and `test/`). Phoenix routers (`use MyAppWeb,
  :router` or `scope "..."`) contribute `get/post/put/patch/delete/
  options/trace` routes with their `scope` path prefixes applied (nested
  scopes concatenate); Plug routers (`use Plug.Router`) contribute
  `get "/path" do` blocks with an empty handler. The router is the source
  of truth — controllers are not scanned.
- Each route becomes `{method, path, handler, source_file}` (handler is
  `Controller.action` as written in the router), capped at 1000 routes.
- Base URL: `DATAFLOW_HTTP_URL`, else the URL-form `DATAFLOW_ENDPOINT`
  (same resolution as the manifest); the API key comes from
  `DATAFLOW_API_KEY` via `Dataflow.configure/0`. Without either, the run
  skips.
- Options: `dir:` (default `"."`), `service:` (default
  `DATAFLOW_SERVICE_NAME`, then the directory name), `url:` override and
  `print: true` to print the catalog JSON instead of posting.

Returns `{:ok, route_count}`, `{:error, reason}` or `:skipped`; it never
raises into the caller.
