defmodule Registry.Mixfile do
  use Mix.Project

  def project do
    [app: :registry,
     version: "0.1.0",
     elixir: "~> 1.3",

     name: "Registry",
     source_url: "https://github.com/elixir-lang/registry",
     homepage_url: "https://github.com/elixir-lang/registry",
     docs: [main: "Registry"],

     build_embedded: Mix.env == :prod,
     start_permanent: Mix.env == :prod,
     deps: deps()]
  end

  # Configuration for the OTP application
  #
  # Type "mix help compile.app" for more information
  def application do
    [applications: [:logger]]
  end

  # Dependencies can be Hex packages:
  #
  #   {:my_dep, "~> 0.3.0"}
  #
  # Or git/path repositories:
  #
  #   {:my_dep, git: "https://github.com/elixir-lang/my_dep.git", tag: "0.1.0"}
  #
  # Type "mix help deps" for more examples and options
  defp deps do
    [{:ex_doc, "~> 0.14", only: :docs},
     {:gproc, "~> 0.6", only: :bench}]
  end
end
