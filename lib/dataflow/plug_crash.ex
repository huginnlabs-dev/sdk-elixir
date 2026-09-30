defmodule Dataflow.PlugCrash do
  @moduledoc """
  Crash capture for Plug routers. `use Dataflow.PlugCrash` (after
  `use Plug.Router`) wraps the router's dispatch: when a handler raises,
  throws or exits, the crash is recorded on the request's span — the same
  convention as `Dataflow.Crash.capture/1` (status 500, formatted error,
  "error.stack" metadata) plus the request line as "error.request" — and
  the exception is re-raised, so Plug's error handling proceeds exactly as
  it would without the SDK.

      defmodule MyApp.Router do
        use Plug.Router
        use Dataflow.PlugCrash

        plug :match
        plug :dispatch

        get "/orders" do
          send_resp(conn, 200, "[]")
        end
      end

  Recording is best-effort (never masks the crash) and skips entirely when
  tracing is disabled. The wrapper mirrors `Plug.ErrorHandler`'s
  `plug_builder_call/2` override, so it composes with error-handling
  wrappers the same way — each layer's `super` call chains to the next.
  """

  defmacro __using__(_opts) do
    quote do
      @before_compile Dataflow.PlugCrash
    end
  end

  defmacro __before_compile__(_env) do
    quote do
      defoverridable plug_builder_call: 2

      def plug_builder_call(conn, opts) do
        try do
          super(conn, opts)
        rescue
          e ->
            Dataflow.Crash.record_exception(:error, e, __STACKTRACE__, conn)
            reraise e, __STACKTRACE__
        catch
          kind, value ->
            Dataflow.Crash.record_exception(kind, value, __STACKTRACE__, conn)
            :erlang.raise(kind, value, __STACKTRACE__)
        end
      end
    end
  end
end
