# Dataflow Elixir SDK

HuginnLabs Dataflow tracing for Elixir/Erlang services: runtime spans with
E2E-encrypted payloads, shipped over the REST ingest API. Spans ride the
process dictionary, so nested `Dataflow.trace/2` calls join the enclosing
trace on the BEAM; a background pipeline batches completed events and
delivers them with ack-based replay.

Requires Elixir ~> 1.18. The only hex dependency is `:telemetry` (for the
Ecto tracer); HTTP uses OTP's built-in `:httpc`, crypto uses `:crypto`,
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
