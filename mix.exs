defmodule LRQC.Mixfile do
  use Mix.Project

  def project do
    [ app: :lqrc,
      version: "0.2.2-dev",
      deps: deps,
      aliases: aliases
    ]
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

  defp aliases do
    ["deps.compile": &fix_riak_pb_deps/1]
  end

  # this shit is the worst, waiting for riakc to tag a new release
  # upstream so we can avoid this fucking shit....
  defp fix_riak_pb_deps(args) do
    System.cmd System.cwd <> "/deps/riak_pb/rebar", ["clean", "compile", "deps_dir= ."], [cd: "./deps/riak_pb"]
    Mix.Task.run "deps.compile", args
  end

end
