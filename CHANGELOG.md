# Changelog

## 0.8.1 — 2026-10-05

First run of the ExUnit suite since it was written (86 tests, now green)
surfaced these fixes:

### Fixed

- **Pipeline crash loop on buffer overflow**: `add_with_cap` bound the
  popped *event* (a JSON binary) into the buffer slot instead of the
  remaining queue, so overflowing the 10 000-event replay buffer raised
  in `:queue.in/2` and killed the flush GenServer — the supervisor
  restarted it, the buffer refilled and crashed again, silently dropping
  every buffered span. Any ingest outage longer than a few seconds under
  load hit this.
- **`Dataflow.HTTP` always failed on recent OTP**: the injected trace-id
  header name was sent as a UTF-8 binary; OTP 27+ `:httpc` rejects it
  with `{:error, {:headers_error, :invalid_field}}` — every traced HTTP
  call failed. Header names are now charlists.
- **`span_name` dropped explicit ports**: `POST host:4567/path` was
  recorded as `POST host/path`; the authority now matches `host/1` (port
  kept when it differs from the scheme default).
- **Oban worker name carried the `Elixir.` prefix** on recent OTP
  (`oban.Elixir.MyApp.Worker`); the bare module path is used again.

### Changed

- The test suite is runnable and green (`mix test`, 86 tests): fixed a
  `~s()` sigil compile error, replaced supervisor child-pid swapping
  (unreliable across OTP releases) with a stop-tree/restore harness, and
  aligned assertions with the shipped BEAM-style error rendering.
