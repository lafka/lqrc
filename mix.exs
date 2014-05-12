defmodule LRQC.Mixfile do
  use Mix.Project

  def project do
    [ app: :lqrc,
      version: "0.1.4-1",
      #elixir: "~> 0.10.1",
      deps: deps ]
  end

  def application do
    [ applications: [ :pooler, :riakc ],
      mod: { LQRC, [] },
      env: []
    ]
  end

  defp deps do
    [ {:riakc, github: "basho/riak-erlang-client", branch: "master"},
      {:jsx, github: "talentdeficit/jsx", tag: "v1.4.4"},
      {:pooler, github: "seth/pooler", tag: "1.0.0"} ]
  end
end
