# curl -XPUT http://localhost:8098/search/index/user
# bin/riak-admin bucket-type create user '{"props":{"search_index":"user"}}'     
# bin/riak-admin bucket-type activate user

ExUnit.start

Code.ensure_loaded LQRC.Riak

LQRC.start :normal, []


defmodule RH do
  def reset do
    {:ok, backend} = :rpc.call testnode, :application, :get_env, [:riak_kv, :storage_backend]
    test?   = :rpc.call testnode, :application, :get_env, [:riak_kv, :test]
    case {backend, test?} do
      {:riak_kv_memory_backend, {:ok, :true}} -> :ok;

      _ ->
        :ok = :rpc.call testnode, :application, :set_env, [:riak_kv, :test, :true]
        :ok = :rpc.call testnode, :application, :set_env, [:riak_kv, :storage_backend, :riak_kv_memory_backend]

    end

    vnodes = :rpc.call testnode, :riak_core_vnode_manager, :all_vnodes, []

    lc {_, _, pid} inlist vnodes do
      :ok = :rpc.call testnode, :supervisor, :terminate_child, [:riak_core_vnode_sup, pid]
    end

    :timer.sleep 25
  end

  def testcookie, do: binary_to_atom (System.get_env("RIAK_TEST_COOKIE") || "riak")
  def testhost, do: binary_to_atom (System.get_env("RIAK_TEST_HOST") || "127.0.0.1")
  def testnode, do: binary_to_atom (System.get_env("RIAK_TEST_NODE") || "riak@127.0.0.1")

  def ensure_conn do
    System.cmd "empd -daemon"
    case :net_kernel.start [:lqrc_eunit, :longnames] do
      {:ok, _} -> :erlang.set_cookie testnode, testcookie; :ok;
      {:error, {:already_started, _}} -> :ok
    end

  end
end

RH.ensure_conn
RH.reset

:pooler.new_pool([{:name, :riak},
  {:group, :riak},
  {:max_count, 1000},
  {:init_count, 10},
  {:start_mfa, {:riakc_pb_socket,
    :start_link,
    [atom_to_list(RH.testhost), 8087]}}
])
