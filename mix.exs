defmodule LRQC.Mixfile do
  use Mix.Project

  def project do
    [ app: :lqrc,
      version: "0.1.5-1",
      #elixir: "~> 0.10.1",
      deps: deps ]
  end

  def application do
    [ applications: [ :pooler, :riakc ],
      mod: { LQRC, [] },
      env: []
    ]
  end

#  def testdataset_path, do: :filename.join("./test/dataset/", project[:version])
   # pattern can either be the exact selector OR if using the `:_`
   # suffix one can create wildcard patterns
#  def testdataset_patterns, do: [[:user, :_]]

  defp deps do
    [ {:riakc, github: "basho/riak-erlang-client", branch: "master"},
      {:jsx, github: "talentdeficit/jsx", tag: "v1.4.4"},
      {:pooler, github: "seth/pooler", tag: "1.0.0"} ]
  end
end
