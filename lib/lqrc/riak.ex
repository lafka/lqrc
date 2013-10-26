defmodule LQRC.Riak.PID do
  @moduledoc """
  Provide primitives to get and return riak pid members
  """

  defmacro __using__(_opts) do
    quote do
      defp with_pid(fun) do
        case :pooler.take_group_member(:riak) do
          pid when is_pid(pid) ->
            fun.(pid) |> return pid

          err ->
              raise "could not get riak pid: #{Kernel.inspect err}"
        end
      end

      defp return(p), do: return(:ok, p)

      defp return({:ok, _} = r, p) do
        :pooler.return_group_member :riak, p, :ok; r end

      defp return(:ok, p) do
        :pooler.return_group_member :riak, p, :ok; :ok end

      defp return({:error, {:notfound, _}} = r, p) do
        :pooler.return_group_member :riak, p, :ok; r
        {:error, :notfound}
      end

      defp return({:error, :notfound} = r, p) do
        :pooler.return_group_member :riak, p, :ok; r end

      defp return({:error, _} = r, p) do
        :pooler.return_group_member :riak, p, :ok; r end
    end
  end
end

defmodule LQRC.Riak do

  use LQRC.Riak.PID

  require :riakc_pb_socket, as: PB

  def write(domain, sel, vals, spec, opts // []) do
    vals = List.keystore vals, "key", 1, {"key", List.last sel}
    modify(domain, sel, vals, spec, &update_map(vals, &1), opts)
  end

  def reduce(domain, sel, vals, spec, opts // []) do
    modify(domain, sel, vals, spec, &reduce_map(vals, &1), opts)
  end

  def read(domain, sel, spec, opts // []) do
    {bucket, key} = genkey sel

    decode = :proplists.get_value :decode, opts, true

    buckettype = atom_to_binary domain
    case with_pid &PB.fetch_type(&1, {buckettype, bucket}, key) do
      {:ok, crdt} when decode ->
        map_crdt(crdt)

      res ->
        res
    end
  end

  def query(domain, q, spec) do
    {:error, :notimplemented}
    #q = Enum.map q, fn({k,v}) -> "#{k}:#{v}" end
    #case with_pid(&PB.search &1, atom_to_binary(domain), q) do
    #  res ->
    #    IO.inspect res
    #    res
    #end
  end

  defp modify(domain, sel, vals, spec, fun, opts // []) do
    {bucket, key} = genkey sel

    return? = Enum.member? opts, :return || opts[:return]
    buckettype = atom_to_binary domain

    case with_pid(&PB.modify_type &1, fun, {buckettype, bucket}, key, []) do
      :ok when return? ->
        read(domain, sel, opts)

      :ok ->
        :ok

      res -> res
    end
  end

  defp update_map(vals, map) when is_list(vals), do:
      Enum.reduce(vals, map, &update_map/2)

  defp update_map({k, v}, acc) when is_integer(v), do:
    :riakc_map.update({k, :counter}, fn(counter) ->
      :riakc_counter.increment v, counter end, acc)

  defp update_map({k, v}, acc) when is_binary(v), do:
    :riakc_map.update({k, :register}, fn(reg) -> :riakc_register.set v, reg end, acc)

  defp update_map({k, v}, acc) when is_float(v) do
    v = iolist_to_binary(:io_lib.format("~g", [v]))
    :riakc_map.update({k, :register}, fn(reg) -> :riakc_register.set v, reg end, acc)
  end

  defp update_map({k, true}, acc), do:
    :riakc_map.update({k, :flag}, fn(flag) -> :riakc_flag.enable flag end, acc)

  defp update_map({k, false}, acc), do:
    :riakc_map.update({k, :flag}, fn(flag) -> :riakc_flag.disable flag end, acc)

  defp update_map({k, [{_,_} | _] = v}, acc), do:
    :riakc_map.update({k, :map}, fn(map) -> update_map(v, map) end, acc)

  defp update_map({k, [_h | _] = v}, acc), do:
    :riakc_map.update({k, :set}, fn(set) ->
      Enum.reduce v, set, fn(v0, a0) ->
        :riakc_set.add_element v0, a0
      end
    end, acc)

  defp reduce_map(vals, map) when is_list(vals), do:
      Enum.reduce(vals, map, &reduce_map/2)

  defp reduce_map({k, [{_,_} | _] = v}, acc) do
    :riakc_map.update({k, :map}, fn(map) -> reduce_map(v, map) end, acc)
  end

  defp reduce_map({k, [_h | _] = v}, acc) do
    :riakc_map.update({k, :set}, fn(set) ->
      Enum.reduce v, set, fn(v0, a0) ->
        :riakc_set.del_element v0, a0
      end
    end, acc) end

  defp reduce_map({k, _}, acc) do
    :riakc_map.erase(k, acc)
  end

  defp map_crdt(crdt) do
    case :riakc_datatype.module_for_term crdt do
      :undefined  ->
        {:error, {:unknown_crdt, crdt}}

      mod ->
        {:ok, mapper(mod, crdt)}
    end
  end

  defp mapper_red({{k, _}, v}) when is_list(v), do:
    {k, lc v0 inlist v do mapper_red(v0) end}
  defp mapper_red({{k, _}, v}), do:
    {k, v}
  defp mapper_red(v), do:
    v

  defp mapper(:riakc_map = m, crdt), do: m.fold(fn(k, v, acc) ->
      [mapper_red({k, v}) | acc]
    end, [], crdt)
  defp mapper(:riakc_set = m, crdt), do: :ordset.to_list m.value(crdt)
  defp mapper(mod, crdt), do:    mod.value crdt


  defp genkey([key]), do: {"_", key}
  defp genkey(sel) do
    {a, [b]} = Enum.split sel, length(sel) - 1
    {Enum.join(a, "/"), b}
  end
end
