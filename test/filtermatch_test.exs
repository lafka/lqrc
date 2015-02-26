
defmodule LqrcTest do
  use ExUnit.Case

  require :riakc_pb_socket, as: PB
  require LQRC.Pooler
  LQRC.Pooler.__using__([group: :riak])

  @tdom [riak: [putopts: [:return_body]]]

  setup_all do
    {:ok, {_container, host}} = LQRC.Test.Utils.Riak.maybe_start

    {:ok, _pool} = :pooler.new_pool([{:name, :riak},
      {:group, :riak},
      {:max_count, 1000},
      {:init_count, 10},
      {:start_mfa, {:riakc_pb_socket,
        :start_link,
        [host, 8087]}}
    ])

    on_exit fn ->
      :pooler.rm_pool(:riak)
    end

    :timer.sleep 1000

    :ok
  end

  test "filtermatch" do
    :ok = LQRC.Domain.write :filtermatch, [
      key: nil,
      schema: %{
        "created" => [type: :str, filtermatch: %{action: [:create]}],
        "updated" => [type: :str, filtermatch: %{action: [:update]}],
      },
      riak: [putopts: [:return_body]]
    ]

    key = genkey
    assert {:ok, %{"created" => "yes"}}
      === LQRC.write :filtermatch, [key], %{"created" => "yes"}, [action: :create]

    assert {:ok, %{"created" => "yes", "updated" => "yes"}}
       === LQRC.update :filtermatch, [key], %{"created" => "no", "updated" => "yes"}, [action: :update]

    assert {:ok, %{"created" => "yes", "updated" => "yes"}}
       === LQRC.read :filtermatch, [key]

  end

  defp genkey, do: :base64.encode(:erlang.term_to_binary :erlang.now)
end
