defmodule Dataflow.MixProject do
  use Mix.Project

  def project do
    [
      app: :dataflow,
      version: "0.8.1",
      elixir: "~> 1.18",
      start_permanent: Mix.env() == :prod,
      deps: [{:telemetry, "~> 1.0"}]
    ]
  end

  # The SDK rides on OTP built-ins (:httpc, AES-GCM/PBKDF2 with :crypto,
  # JSON with Elixir 1.18+); :telemetry is the single hex dep, needed for
  # the Ecto and Oban tracers.
  def application do
    [extra_applications: [:logger, :inets, :ssl, :crypto], mod: {Dataflow, []}]
  end
end
