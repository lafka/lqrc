defmodule LRQC.Mixfile do
  use Mix.Project

  def project do
    [ app: :lqrc,
      version: "0.1.0",
      #elixir: "~> 0.10.1",
      deps: deps ]
  end

  def application do
    [ applications: [ :pooler ],
      env: []
    ]
  end

  defp deps do
    [ {:riakc, github: "basho/riak-erlang-client", branch: "master"},
      {:pooler, github: "seth/pooler", tag: "1.0.0"} ]
  end
end
