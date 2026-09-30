defmodule Dataflow.MixProject do
  use Mix.Project

  def project do
    [
      app: :dataflow,
      version: "0.2.0",
      elixir: "~> 1.18",
      start_permanent: Mix.env() == :prod,
      deps: []
    ]
  end

  # The SDK is dependency-free on purpose: :httpc ships with Erlang/OTP,
  # AES-GCM/PBKDF2 with :crypto, JSON with Elixir 1.18+.
  def application do
    [extra_applications: [:logger, :inets, :ssl, :crypto], mod: {Dataflow, []}]
  end
end
