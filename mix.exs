defmodule Valvula.MixProject do
  use Mix.Project

  @version "0.1.0"
  @source_url "https://github.com/Lorenzo-SF/valvula"

  def project do
    [
      app: :valvula,
      version: @version,
      elixir: "~> 1.14",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description: description(),
      package: package(),
      docs: docs(),
      source_url: @source_url,
      homepage_url: @source_url
    ]
  end

  def application do
    [extra_applications: [:logger]]
  end

  defp description do
    "Token-bucket rate limiter for Elixir with ETS backend and OTP-native " <>
      "GenServer - zero external dependencies."
  end

  defp deps do
    [
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, ">= 1.0.0", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.34", only: :dev, runtime: false}
    ]
  end

  defp package do
    [
      files: ["lib", "mix.exs", "README.md", "README_ES.md", "LICENSE.md"],
      maintainers: ["Lorenzo Sánchez"],
      licenses: ["MIT"],
      links: %{"GitHub" => @source_url}
    ]
  end

  defp docs do
    [
      main: "readme",
      extras: ["README.md", "docs/README.es.md", "LICENSE.md", "CHANGELOG.md"],
      source_url: "https://github.com/Lorenzo-SF/valvula",
      homepage_url: "https://github.com/Lorenzo-SF/valvula",
      source_ref: "v0.1.0"
    ]
  end
end
