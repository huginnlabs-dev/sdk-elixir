defmodule Dataflow.ScanTest do
  use ExUnit.Case, async: false

  @phoenix_router """
  defmodule MyAppWeb.Router do
    use MyAppWeb, :router

    pipeline :api do
      plug :accepts, ["json"]
    end

    scope "/api", MyAppWeb do
      pipe_through :api

      get "/orders", OrderController, :index
      post "/orders", OrderController, :create
      get "/orders/:id", OrderController, :show
      put "/orders/:id", OrderController, :update
      patch "/orders/:id", OrderController, :rename
      delete "/orders/:id", OrderController, :delete
      options "/orders", OrderController, :options
      trace "/orders", OrderController, :trace
    end

    scope "/", PageScope do
      get "/", PageController, :home
      get "/about", PageController, :about
    end
  end
  """

  @plug_router """
  defmodule MyApp.PlugRouter do
    use Plug.Router

    plug :match
    plug :dispatch

    get "/health" do
      send_resp(conn, 200, "ok")
    end

    post "/events" do
      send_resp(conn, 201, "created")
    end

    match _ do
      send_resp(conn, 404, "not found")
    end
  end
  """

  describe "extract/1" do
    test "phoenix router: scope prefixes, upcased methods, handlers as written" do
      routes = Dataflow.Scan.extract([{"lib/my_app_web/router.ex", @phoenix_router}])

      assert routes == [
               route("GET", "/api/orders", "OrderController.index"),
               route("POST", "/api/orders", "OrderController.create"),
               route("GET", "/api/orders/:id", "OrderController.show"),
               route("PUT", "/api/orders/:id", "OrderController.update"),
               route("PATCH", "/api/orders/:id", "OrderController.rename"),
               route("DELETE", "/api/orders/:id", "OrderController.delete"),
               route("OPTIONS", "/api/orders", "OrderController.options"),
               route("TRACE", "/api/orders", "OrderController.trace"),
               route("GET", "/", "PageController.home"),
               route("GET", "/about", "PageController.about")
             ]
    end

    test "a file with only scope lines counts as a Phoenix router" do
      source = """
      scope "/api" do
        get "/ping", PingController, :ping
      end
      """

      assert [%{"method" => "GET", "path" => "/api/ping", "handler" => "PingController.ping", "source_file" => "lib/api.ex"}] =
               Dataflow.Scan.extract([{"lib/api.ex", source}])
    end

    test "nested scope prefixes concatenate" do
      source = """
      scope "/api" do
        scope "/v1" do
          get "/users", UserController, :index
        end
      end
      """

      assert [%{"path" => "/api/v1/users"}] = Dataflow.Scan.extract([{"lib/router.ex", source}])
    end

    test "route paths without a leading slash are normalized" do
      source = """
      use MyAppWeb, :router

      get "orders", OrderController, :index
      """

      assert [%{"method" => "GET", "path" => "/orders", "handler" => "OrderController.index"}] =
               Dataflow.Scan.extract([{"lib/router.ex", source}])
    end

    test "plug router do-blocks yield empty handlers" do
      routes = Dataflow.Scan.extract([{"lib/my_app/plug_router.ex", @plug_router}])

      assert routes == [
               %{"method" => "GET", "path" => "/health", "handler" => "", "source_file" => "lib/my_app/plug_router.ex"},
               %{"method" => "POST", "path" => "/events", "handler" => "", "source_file" => "lib/my_app/plug_router.ex"}
             ]
    end

    test "plain modules and Phoenix controllers yield no routes" do
      controller = """
      defmodule MyAppWeb.OrderController do
        use MyAppWeb, :controller

        def show(conn, %{"id" => id}) do
          json(conn, %{id: id})
        end
      end
      """

      plain = """
      defmodule MyApp.RouteNotes do
        # get "/orders", OrderController, :index

        def describe do
          "handled by the router"
        end
      end
      """

      assert Dataflow.Scan.extract([
               {"lib/my_app_web/controllers/order_controller.ex", controller},
               {"lib/my_app/route_notes.ex", plain}
             ]) == []
    end

    test "malformed input yields [] instead of raising" do
      assert Dataflow.Scan.extract(nil) == []
      assert Dataflow.Scan.extract([{"lib/broken.ex", :not_a_source}]) == []
    end
  end

  describe "catalog/2" do
    test "builds the exact POST body map" do
      routes = [route("GET", "/api/orders", "OrderController.index")]

      assert Dataflow.Scan.catalog("payments", routes) == %{
               "service_name" => "payments",
               "routes" => routes
             }
    end

    test "body round-trips through the built-in JSON encoder" do
      body = Dataflow.Scan.catalog("svc", [])
      assert is_binary(JSON.encode!(body))
    end
  end

  describe "run/1" do
    test "print mode returns the route count and prints the catalog JSON" do
      dir = Path.join(System.tmp_dir!(), "dataflow_scan_test_#{System.unique_integer([:positive])}")

      File.mkdir_p!(Path.join(dir, "lib"))
      File.write!(Path.join(dir, "lib/router.ex"), @phoenix_router)
      on_exit(fn -> File.rm_rf!(dir) end)

      output =
        ExUnit.CaptureIO.capture_io(fn ->
          assert {:ok, 10} = Dataflow.Scan.run(dir: dir, service: "scan_test", print: true)
        end)

      assert output =~ ~s("service_name":"scan_test")
      assert output =~ ~s("/api/orders/:id")
    end

    test "posting is skipped without a derivable HTTP base or API key" do
      Application.put_env(:dataflow, :settings, %{endpoint: "api:9090", api_key: ""})
      on_exit(fn -> Application.delete_env(:dataflow, :settings) end)
      System.delete_env("DATAFLOW_HTTP_URL")

      assert Dataflow.Scan.run(dir: "lib") == :skipped
    end

    test "failures return {:error, reason} instead of raising" do
      assert {:error, _reason} = Dataflow.Scan.run(dir: "lib", service: %{not: "a_string"})
    end
  end

  defp route(method, path, handler, file \\ "lib/my_app_web/router.ex") do
    %{"method" => method, "path" => path, "handler" => handler, "source_file" => file}
  end
end
