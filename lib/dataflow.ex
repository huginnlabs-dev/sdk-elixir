defmodule Dataflow do
  @moduledoc """
  HuginnLabs Dataflow SDK for Elixir — runtime tracing with E2E-encrypted
  payloads.

  Spans ride the process dictionary, so nested `trace/2` calls join the
  enclosing trace naturally on the BEAM. A background GenServer batches
  completed events and ships them to the REST ingest endpoint; the buffer
  is trimmed by the acked `last_seq`. Values are AES-256-GCM encrypted with
  a PBKDF2-derived key (:crypto) that never leaves the node.

      Dataflow.configure()
      Dataflow.attach_ecto()                  # DB_QUERY spans for Ecto repos
      Dataflow.attach_oban()                  # FUNCTION_CALL spans for Oban jobs
      Dataflow.trace("ingest.Validate", fn span ->
        Dataflow.Span.data(span, "event_id", ev.id)
        Dataflow.trace("schema.Check", fn s -> ... end)
      end)
      Dataflow.HTTP.get(url)                  # HTTP_CLIENT span + trace id header
      Dataflow.info("shipped", %{"size" => n})  # log line joined to the current trace
      Dataflow.attach_logger()                # forward Logger messages as logs
      Dataflow.capture(fn -> risky() end)     # crash → error.stack, then re-raise
  """

  use Application
  require Logger

  @sdk_version "0.8.1"
  @key_len 32
  @salt_len 16
  @iterations 10_000

  # --- application bootstrap ------------------------------------------------

  @impl true
  def start(_type, _args) do
    configure()

    # Idempotent: when :dataflow runs as a path dep of a demo app its
    # start callback already ran once — never spawn a second Pipeline.
    case Process.whereis(Dataflow.Supervisor) do
      nil ->
        # Report the service manifest once, from the same point the pipeline
        # starts; best-effort, independent of the tracing pipeline. The
        # whereis guard keeps it exactly-once across double starts.
        Dataflow.Manifest.send_manifest()
        Supervisor.start_link([Dataflow.Pipeline, Dataflow.Logs], strategy: :one_for_one, name: Dataflow.Supervisor)

      _pid ->
        :ignore
    end
  end

  # --- configuration ----------------------------------------------------------

  @doc "Reads DATAFLOW_* env and derives the payload key once per node."
  def configure do
    settings = %{
      endpoint: env("DATAFLOW_ENDPOINT", ""),
      api_key: env("DATAFLOW_API_KEY", ""),
      service_name: env("DATAFLOW_SERVICE_NAME", ""),
      sample_ratio: env_float("DATAFLOW_SAMPLE_RATIO", 1.0),
      buffer_size: env_float("DATAFLOW_BUFFER_SIZE", 10_000) |> trunc(),
      disabled: env("DATAFLOW_DISABLED", "false") == "true"
    }

    Application.put_env(:dataflow, :settings, settings)

    case env("DATAFLOW_ENCRYPTION_KEY", "") do
      "" ->
        Logger.warning("dataflow: no encryption key set; captured payloads are sent as plaintext")

        Application.put_env(:dataflow, :envelope, nil)

      secret ->
        salt = :crypto.strong_rand_bytes(@salt_len)
        key = :crypto.pbkdf2_hmac(:sha256, secret, salt, @iterations, @key_len)
        Application.put_env(:dataflow, :envelope, %{
          key: key,
          salt_hex: Base.encode16(salt, case: :lower)
        })
    end

    :ok
  end

  def enabled? do
    s = Application.get_env(:dataflow, :settings, %{})
    !Map.get(s, :disabled, true) && s.endpoint != "" && s.api_key != ""
  end

  def service_name do
    s = Application.get_env(:dataflow, :settings, %{})
    case Map.get(s, :service_name, "") do
      "" ->
        {:ok, host} = :inet.gethostname()
        to_string(host)

      name ->
        name
    end
  end

  def sdk_version, do: @sdk_version

  defp env(k, fallback), do: System.get_env(k) || fallback
  defp env_float(k, fallback), do: (System.get_env(k) && case System.get_env(k) |> Float.parse() do {v, _} -> v; :error -> fallback end) || fallback

  # --- span context (process dictionary, like contextvars on the BEAM) -------

  @doc "Runs `fun` inside a named span joined to the current trace."
  def trace(name, fun) when is_function(fun, 1) do
    span = start_span(name)
    try do
      fun.(span)
    rescue
      e ->
        Dataflow.Span.record_error(span, Exception.message(e) || inspect(e))
        reraise e, __STACKTRACE__
    catch
      kind, value ->
        Dataflow.Span.record_error(span, inspect({kind, value}))
        throw value
    after
      end_current_span()
    end
  end

  @doc "Opens a child span of the current process's span (or a new trace)."
  def start_span(name, type \\ "FUNCTION_CALL") do
    parent = current_span()
    span = Dataflow.Span.new(name, type, parent)
    push_span(span)
    span
  end

  @doc "Opens an entry-point span, adopting the incoming X-Dataflow-Trace-Id."
  def start_server_span(route, incoming_trace_id \\ nil) do
    span = Dataflow.Span.new(route, "HTTP_SERVER", nil, incoming_trace_id)
    push_span(span)
    Enum.each(agent_attrs(), fn {k, v} -> Dataflow.Span.attr(span, k, v) end)
    span
  end

  # Elixir maps are immutable, so live span state lives in a per-process
  # stack keyed by span_id; Span mutators update the stack copy in place.
  defp push_span(span),
    do: Process.put(:dataflow_stack, [{span.span_id, span} | Process.get(:dataflow_stack) || []])

  def current_span do
    case Process.get(:dataflow_stack) || [] do
      [{_id, span} | _] -> span
      [] -> nil
    end
  end

  @doc false
  def update_span(span_id, fun) do
    stack = Process.get(:dataflow_stack) || []

    stack =
      case List.keyfind(stack, span_id, 0) do
        {id, span} -> List.keyreplace(stack, id, 0, {id, fun.(span)})
        nil -> stack
      end

    Process.put(:dataflow_stack, stack)
    :ok
  end

  @doc "Ends the innermost active span."
  def end_current_span do
    case Process.get(:dataflow_stack) || [] do
      [{_id, span} | rest] ->
        Process.put(:dataflow_stack, rest)
        Dataflow.Span.end_span(span)

      [] ->
        :ok
    end
  end

  @doc "Clears the process's span context (use at the end of request handling)."
  def clear_context, do: Process.delete(:dataflow_stack)

  # --- crash capture -----------------------------------------------------------

  @doc """
  Crash capture with stack traces: wraps a zero-arity function; any
  raise/throw/exit is recorded on the current span (or a synthetic
  "exception" span) — status 500, the formatted error, the "error.stack"
  metadata entry — and propagated unchanged. Plug routers capture crashes
  with `use Dataflow.PlugCrash`. See `Dataflow.Crash`.
  """
  defdelegate capture(fun), to: Dataflow.Crash

  # --- log capture ---------------------------------------------------------------

  @doc """
  Records an application log line at `level` ("debug"/"info"/"warn"/"error")
  with the current span's trace/span ids and ships it batched to the REST
  logs endpoint. Fields are stringified and capped at 50 entries.
  Best-effort: never raises, no-op with the SDK disabled. See `Dataflow.Logs`.
  """
  defdelegate log(level, message, fields \\ %{}), to: Dataflow.Logs

  @doc "Records a debug-level log line (see `log/3`)."
  defdelegate debug(message, fields \\ %{}), to: Dataflow.Logs

  @doc "Records an info-level log line (see `log/3`)."
  defdelegate info(message, fields \\ %{}), to: Dataflow.Logs

  @doc "Records a warn-level log line (see `log/3`)."
  defdelegate warn(message, fields \\ %{}), to: Dataflow.Logs

  @doc "Records an error-level log line (see `log/3`)."
  defdelegate error(message, fields \\ %{}), to: Dataflow.Logs

  @doc "Flushes buffered log lines now (best-effort; also useful before shutdown)."
  defdelegate flush_logs(), to: Dataflow.Logs, as: :flush

  @doc """
  Installs the Erlang `:logger` handler that forwards `Logger` messages
  into the same batched log pipeline (`warning` → `warn`, metadata →
  fields where stringifiable). Returns `:ok`, or `{:error, :already_exists}`
  when already attached.
  """
  defdelegate attach_logger(), to: Dataflow.Logs

  @doc "Removes the handler installed by `attach_logger/0`."
  defdelegate detach_logger(), to: Dataflow.Logs

  # --- optional integrations ---------------------------------------------------

  @doc """
  Attaches the Ecto query tracer: every `[:<prefix>, :repo, :query]`
  telemetry event becomes a DB_QUERY span (see `Dataflow.Ecto`). Returns
  `:ok`, or `{:error, :already_exists}` when the same prefix is already
  attached.
  """
  def attach_ecto(prefix \\ [:dataflow_sample]), do: Dataflow.Ecto.attach(prefix)

  @doc "Detaches the Ecto query tracer installed by `attach_ecto/1`."
  def detach_ecto(prefix \\ [:dataflow_sample]), do: Dataflow.Ecto.detach(prefix)

  @doc """
  Attaches the Oban job tracer: every `[:<prefix>, :job, :start | :stop |
  :exception]` telemetry event becomes a FUNCTION_CALL span (see
  `Dataflow.Oban`). Returns `:ok`, or `{:error, :already_exists}` when the
  same prefix is already attached.
  """
  def attach_oban(prefix \\ [:oban]), do: Dataflow.Oban.attach(prefix)

  @doc "Detaches the Oban job tracer installed by `attach_oban/1`."
  def detach_oban(prefix \\ [:oban]), do: Dataflow.Oban.detach(prefix)

  defp agent_attrs do
    arch =
      :erlang.system_info(:system_architecture)
      |> to_string()
      |> String.split("-")
      |> hd()
      |> then(fn
        "x86_64" -> "amd64"
        "aarch64" -> "arm64"
        other -> other
      end)

    base = [
      {"agent.os", "#{:os.type() |> elem(1)}/#{arch}"},
      {"agent.runtime", "Elixir #{System.version()} / OTP #{System.otp_release()}"},
      {"agent.sdk", "elixir-sdk/#{@sdk_version}"},
      {"agent.cpu", "#{:erlang.system_info(:logical_processors_available)}"},
      {"agent.pid", self() |> :erlang.pid_to_list() |> to_string()},
      {"agent.started", "#{System.system_time(:millisecond)}"}
    ]

    extra =
      [
        {"DATAFLOW_ENV", "agent.env"},
        {"DATAFLOW_APP_VERSION", "agent.app_version"}
      ]
      |> Enum.flat_map(fn {k, meta} ->
        case System.get_env(k) do
          v when v in [nil, ""] -> []
          v -> [{meta, v}]
        end
      end)

    base ++ extra
  end
end
