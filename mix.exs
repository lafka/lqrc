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
      {:json, github: "talentdeficit/json", ref: "5eecb423e04124f74e4402923c47fe4f4d2f5653"},
      {:json_patch_tests, github: "json-patch/json-patch-tests", app: false, compile: false, override: true},
      {:pooler, github: "seth/pooler", tag: "1.0.0"}
    ]
  end
end
