defmodule Dataflow.HTTPTest do
  use ExUnit.Case, async: false

  alias Dataflow.HTTP

  setup_all do
    {:ok, _} = Application.ensure_all_started(:inets)
    :ok
  end

  setup do
    {:ok, port} = EchoServer.start()

    Application.put_env(:dataflow, :settings, %{
      endpoint: "http://127.0.0.1:1",
      api_key: "test-key",
      service_name: "http-test",
      sample_ratio: 1.0,
      buffer_size: 10_000,
      disabled: false
    })

    on_exit(fn -> Application.delete_env(:dataflow, :settings) end)
    {:ok, port: port}
  end

  describe "span derivation (pure)" do
    test "name is METHOD host/path; host keeps explicit ports" do
      assert HTTP.span_name(:get, "https://api.example.com/v1/orders") == "GET api.example.com/v1/orders"
      assert HTTP.span_name(:post, "http://127.0.0.1:4567/events") == "POST 127.0.0.1:4567/events"
      assert HTTP.host("https://api.example.com/v1/orders") == "api.example.com"
      assert HTTP.host("http://127.0.0.1:4567/events") == "127.0.0.1:4567"
      assert HTTP.host("not a url") == ""
    end
  end

  test "GET emits an HTTP_CLIENT span joined to the current trace and injects the trace id", %{port: port} do
    outer =
      Dataflow.trace("outer", fn span ->
        assert {:ok, 200, _headers, body} = HTTP.get("http://127.0.0.1:#{port}/ping")
        assert body =~ "GET /ping HTTP/1.1"
        assert body =~ "x-dataflow-trace-id: #{span.trace_id}"
        span
      end)

    assert is_binary(outer.trace_id) and outer.trace_id != ""
  end

  test "POST sends body and content type through :httpc", %{port: port} do
    assert {:ok, 200, _headers, body} = HTTP.post("http://127.0.0.1:#{port}/events", [], ~s({"a":1}))
    assert body =~ "POST /events HTTP/1.1"
    assert body =~ ~s({"a":1})
    assert String.downcase(body) =~ "content-type: application/json"
  end

  test "connection failures return {:error, reason} and never raise" do
    assert {:error, _reason} =
             HTTP.request(:get, "http://127.0.0.1:1/nope", [], "", timeout: 1_000, connect_timeout: 1_000)
  end

  test "with tracing disabled the call is a plain pass-through (no header injected)", %{port: port} do
    Application.put_env(:dataflow, :settings, %{
      endpoint: "http://127.0.0.1:1",
      api_key: "test-key",
      service_name: "http-test",
      sample_ratio: 1.0,
      buffer_size: 10_000,
      disabled: true
    })

    assert {:ok, 200, _headers, body} = HTTP.get("http://127.0.0.1:#{port}/plain")
    assert body =~ "GET /plain HTTP/1.1"
    refute body =~ "x-dataflow-trace-id"
  end

  # Minimal HTTP/1.1 echo server on an ephemeral loopback port: every
  # request is answered 200 with the raw request text as the body, so tests
  # can assert on the method line, headers and body exactly as :httpc sent
  # them. Dies with the test process (Task.start link).
  defmodule EchoServer do
    @moduledoc false

    def start do
      {:ok, listen} =
        :gen_tcp.listen(0, [:binary, packet: :raw, active: false, reuseaddr: true, ip: {127, 0, 0, 1}])

      {:ok, port} = :inet.port(listen)
      {:ok, _acceptor} = Task.start(fn -> accept(listen) end)
      {:ok, port}
    end

    defp accept(listen) do
      {:ok, sock} = :gen_tcp.accept(listen)
      serve(sock)
      accept(listen)
    end

    defp serve(sock) do
      case read(sock, <<>>) do
        {:ok, req} ->
          body = "REQ:" <> req

          :gen_tcp.send(
            sock,
            "HTTP/1.1 200 OK\r\ncontent-type: text/plain\r\ncontent-length: #{byte_size(body)}\r\nconnection: close\r\n\r\n" <>
              body
          )

        {:error, _reason} ->
          :ok
      end

      :gen_tcp.close(sock)
    end

    # Reads until the head terminator, then any content-length body.
    defp read(sock, buf) do
      case :binary.match(buf, "\r\n\r\n") do
        {pos, _len} ->
          head = binary_part(buf, 0, pos)
          rest = binary_part(buf, pos + 4, byte_size(buf) - pos - 4)

          case content_length(head) - byte_size(rest) do
            missing when missing > 0 ->
              case :gen_tcp.recv(sock, missing, 5_000) do
                {:ok, tail} -> {:ok, head <> "\r\n\r\n" <> rest <> tail}
                {:error, reason} -> {:error, reason}
              end

            _ ->
              {:ok, head <> "\r\n\r\n" <> rest}
          end

        :nomatch ->
          case :gen_tcp.recv(sock, 0, 5_000) do
            {:ok, data} -> read(sock, buf <> data)
            {:error, reason} -> {:error, reason}
          end
      end
    end

    defp content_length(head) do
      head
      |> String.split("\r\n")
      |> Enum.find_value(0, fn line ->
        case String.downcase(line) do
          "content-length: " <> n ->
            case Integer.parse(n) do
              {value, _rest} -> value
              :error -> 0
            end

          _other ->
            nil
        end
      end)
    end
  end
end
