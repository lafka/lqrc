defmodule LRQC.Mixfile do
  use Mix.Project

  def project do
    [ app: :lqrc,
      version: "0.2.1",
      deps: deps ]
  end

  def application do
    [ applications: [ :pooler, :riakc ],
      mod: { LQRC, [] },
      env: []
    ]
  end

#  def testdataset_path, do: :filename.join("./test/dataset/", project[:version])
#  def testdataset_patterns, do: nil

  defp deps do
    [ {:riakc, github: "basho/riak-erlang-client", branch: "master"},
      {:jsx, "~> 2.0.4", override: true},
      {:jsxn, "~> 0.2.1"},
      {:pooler, github: "seth/pooler", tag: "1.0.0"},
      {:erldocker, github: "proger/erldocker", only: :test, env: :test}]
  end
end
