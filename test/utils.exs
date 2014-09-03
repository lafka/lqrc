defmodule LQRC.Test.Utils do
  def init do
    {:ok, _} = Application.ensure_all_started :erldocker
    maybe_set_endpoint System.get_env("DOCKER_HOST")
  end

  def randomatom, do:
    String.to_atom(Base.encode64 Float.to_string :random.uniform)

  defp maybe_set_endpoint(endpoint) do
    case Application.get_env(:erldocker, :docker_http) do
      ^endpoint ->
        :ok

      _ ->
        IO.puts "docker: update Docker API to #{endpoint}"
        Application.put_env :erldocker, :docker_http, endpoint
    end
  end

  defmodule Redis do
    def maybe_start(image \\ "redis:latest", killonexit? \\ true) do
      case System.get_env "REDIS_HOST" do
        nil ->
          {:ok, cid} = LQRC.Test.Utils.Docker.start image, killonexit?
          {:ok, {cid, Docker.get_ip(cid)}}

        host ->
          {:ok, {nil, :erlang.binary_to_list(host)}}
      end
    end
  end

  defmodule Riak do
    def maybe_start(image \\ "riak:2.0rc1", killonexit? \\ true) do
      case System.get_env "RIAK_HOST" do
        nil ->
          {:ok, cid} = LQRC.Test.Utils.Docker.start image, killonexit?
          {:ok, addr} = wait_for_riak cid
          {:ok, {cid, addr}}

        host ->
          {:ok, {nil, :erlang.binary_to_list(host)}}
      end

    end

    defp wait_for_riak(cid) do
      addr = Docker.get_ip cid
      :timer.sleep 15000

      Process.flag :trap_exit, true
      true = Process.flag :trap_exit, true
      wait_for_riak2 cid, addr
    end

    defp wait_for_riak2(cid, addr) do
      IO.puts "\nconnecting to #{cid} @ #{addr}:8087"
      case :riakc_pb_socket.start_link addr, 8087 do
        {:ok, pid} ->
          IO.inspect :connected
          IO.inspect(:riakc_pb_socket.get(pid, "groceries", "mine"))
          {:ok, addr}
          #case :riakc_pb_socket.pid do
          #  {:ok, []} ->
          #    IO.inspect :ping_pong_china_chong
          #    :riakc_pb_socket.stop pid
          #    {:ok, addr}

          #  x ->
          #    IO.inspect {:alive_but_waiting, x}
          #    :timer.sleep 1000
          #    wait_for_riak2 cid, addr
          #end

        {:error, _} ->
          :timer.sleep 1000
          wait_for_riak2 cid, addr
      end
    end
  end

  defmodule Docker do
    @moduledoc """
    Helper functions to run ad-hoc docker containers
    """

    def command, do: "docker"

    def start(image, killonexit? \\ true) do
      {:ok, res} = :erldocker_api.post([:containers, :create],
        [],
        :jsx.encode(%{
          :Hostname => "test.riak.dev",
          :Image => image,
          :Tty => :true
        }))

      cid = :proplists.get_value "Id", res

      killonexit? && System.at_exit fn(_) -> LQRC.Test.Utils.Docker.kill cid end

      IO.puts "\ndocker: new image #{image} - #{cid}"

      {:ok, {204, ""}} = :erldocker_api.post [:containers, cid, :start], [], "{}"

      {:ok, cid}
    end

    def stop(cid, delete? \\ true) do
      IO.puts "\ndocker: stopping #{cid}"
      :docker_container.stop(cid)

      if delete? do
        {:ok, _} = :erldocker_api.delete [:containers, cid], []
        IO.puts "\ndocker: deleted #{cid}"
      end
    end

    def kill(cid, delete? \\ true) do
      IO.puts "\ndocker: killing #{cid}"
      {:ok, _} = :erldocker_api.post [:containers, cid, :kill], [], ""

      if delete? do
        {:ok, _} = :erldocker_api.delete [:containers, cid], []
        IO.puts "\ndocker: deleted #{cid}"
      end
    end

    def get_ip(cid) do
      {:ok, json} = :erldocker_api.get [:containers, cid, :json], []
      body = :jsxn.decode :jsx.encode(json)
      :erlang.binary_to_list(body["NetworkSettings"]["IPAddress"])
    end
  end
end

LQRC.Test.Utils.init
