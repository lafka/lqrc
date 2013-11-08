defmodule LQRC.Riak do

  require :riakc_pb_socket, as: PB

  require LQRC.Pooler
  LQRC.Pooler.__using__([group: :riak])

  def write(domain, sel, vals, spec, opts) do
    vals = List.keystore vals, "key", 1, {"key", List.last sel}

    if spec[:datatype] do
      modify domain, sel, spec, __MODULE__.CRDT.update(vals, spec), opts
    else
      modify domain, sel, spec, __MODULE__.Obj.update(vals, spec), opts
    end
  end

  def reduce(domain, sel, vals, spec, opts) do
    if spec[:datatype] do
      modify domain, sel, spec, __MODULE__.CRDT.reduce(vals, spec), opts
    else
      modify domain, sel, spec, __MODULE__.Obj.reduce(vals, spec), opts
    end
  end

  @doc """
  Read `sel` in `domain`

  Based on the domain type fetches a CRDT or riak object and maybe
  decodes it.

  Opts can be one of:
    riak:   [term()]  %% Terms to pass to the riak function
    pid:    pid()     %% Pid to use instead of fetching one from pooler
    decode: boolean() %% Whetever to decode the value return from riak pb
  """
  def read(domain, sel, spec, opts) do
    {bucket, key} = genkey domain, sel, spec

    decode = :proplists.get_value :decode, opts, true

    fun = if spec[:datatype] do
        &PB.fetch_type(&1, bucket, key, opts[:readopts] || [])
      else
        &PB.get(&1, bucket, key, opts[:readopts] || [])
    end

    with_pid fun, opts[:pid], fn
      ({:ok, ret}, p) ->
        return p

        if spec[:datatype] do
          __MODULE__.CRDT.decode(ret)
        else
          __MODULE__.Obj.decode(ret)
        end

      (ret, p) ->
        return ret, p
    end
  end

  defp modify(domain, sel, spec, fun, opts) do
    {bucket, key} = genkey domain, sel, spec

    return? = opts[:return]
    merge?  = spec[:merge] || false

    modfun = if spec[:datatype] do
        &PB.modify_type &1, fun, bucket, key, opts[:writeopts] || []
      else
        fn(p) ->
          case PB.get(p, bucket, key) do
            {:ok, obj} ->
              obj = fun.(obj)

              # in case of siblings, select the first and let decode
              # merge all the values as required.
              if merge? && :riakc_obj.value_count obj > 1 do
                obj = :riakc_obj.select_sibling(1, obj)
              end

              PB.put p, obj

            {:error, :notfound} ->
              obj = fun.(:riakc_obj.new(bucket, key, "", []))
              PB.put p, obj

            ret ->
              ret
        end
      end
    end

    with_pid modfun, opts[:pid], fn
      (:ok, pid) when return? ->
        read(domain, sel, spec, [{:pid, pid} | opts])

      (res, pid) ->
        return res, pid
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

  def range(domain, sel, a, b, spec, opts) do
    {bucket, idx} = genkey domain, sel, spec

    idx = maybe_expand_idx(idx, spec)

    fun = if opts[:expand] do
        input = {:index, bucket, idx, a, b}
        phases = [
          {:map, {:modfun, :lqrc_mr, :robject}, :get_values, true}]

        &PB.mapred(&1, input, phases)
      else
        &PB.get_index_range(&1, bucket, idx, a, b)
      end

    with_pid fun, opts[:pid], fn
      ({:ok, [{0, vals}]}, pid) ->
        return :ok, pid
        {:ok, __MODULE__.Obj.decode(vals, false)}

      ({:ok, {:index_results_v1, keys, _, _}}, pid) ->
        return :ok, pid
        {:ok, keys}

      (res, pid) ->
        return res, pid
    end
  end

  def index(domain, sel, val, spec, opts) do
    {bucket, idx} = genkey domain, sel, spec

    idx = maybe_expand_idx(idx, spec)

    # to map/reduce or not to map/reduce
    fun = if opts[:expand] do
        input = {:index, bucket, idx, val}
        phases = [
          {:map, {:modfun, :lqrc_mr, :robject}, :get_values, true}]

        &PB.mapred(&1, input, phases)
      else
        &PB.get_index_eq(&1, bucket, idx, val)
      end

    with_pid fun, opts[:pid], fn
      ({:ok, [{0, vals}]}, pid) ->
        return :ok, pid
        {:ok, __MODULE__.Obj.decode(vals, false)}

      ({:ok, {:index_results_v1, keys, _, _}}, pid) ->
        return :ok, pid
        {:ok, keys}

      (res, pid) ->
        return res, pid
    end
  end

  defp maybe_expand_idx(idx, spec) do
    case (lc {_, ^idx} = i inlist spec[:index] do i end) do
      []  -> idx
      [{t, k}|_] ->
        {:ok, str} = String.to_char_list k
        {t, str}
    end
  end

  @doc """
  Create the riak {bucket, key} pair.

  Bucket will, depending on spec[:datatype], be either a tuple {domain, bucket}
  or binary representation of the same `\#{domain}/\#{bucket}`

  If called without spec domain will not be prefixed
  """
  defp genkey(domain, sel, spec // nil) do
    case sel do
      [key] ->
        {atom_to_binary(domain), key}

      sel when spec === nil ->
        {a, [b]} = Enum.split sel, length(sel) - 1
        {Enum.join(a, "/"), b}

      sel ->
        {a, [b]} = Enum.split sel, length(sel) - 1
        {Enum.join([atom_to_binary(domain) | a], "/"), b}
    end
  end

  defmodule Obj do
    @moduledoc """
    Bindings for future riakc_obj updates

    All functions in this module are sideeffect free and caller must
    save the data using the appropriate function in :riakc_pb_socket API.
    """

    @doc """
    Closure for updating riak objects
    """
   def update(vals, spec) do
      fn(obj) ->
        vals = cond do
          spec[:merge_updates] -> ukeymergerec(vals, decode(obj))
                          true -> vals
        end

        # Add indexes if they exist
        IO.inspect {:spec, spec}
        md = case spec[:index] do
          [_|_] = indexes ->
            md = :riakc_obj.get_update_metadata obj
            md = Enum.reduce indexes, md, fn({t, k} = idx, acc) ->
                IO.inspect {:idx, idx, vals[k]}
                case vals[k] do
                  [_|_] = vals -> :riakc_obj.add_secondary_index(acc, {idx, vals})
                  nil -> raise "no such index: #{spec[:domain]}:#{idx}"
                  val -> :riakc_obj.add_secondary_index(acc, {idx, [val]})
                end
            end
            obj = :riakc_obj.update_metadata obj, md

          _ ->
            obj
        end

        :riakc_obj.update_value(obj, term_to_binary(vals))
      end
    end

    @doc """
    Recursively merge the unsorted lists A and B, picking the last
    value from list A if there are any duplicates.
    """
    def ukeymergerec([], b), do: b
    def ukeymergerec([{k, v} | rest], b) do
      set? = is_list(v) && length(v) > 0 && is_binary(hd(v))
      v = case b[k] do
        oldval when !set? and is_list(v) ->
          ukeymergerec(v, oldval || [])

        oldval when set? and is_list(v) and is_list(oldval) ->
          :lists.usort(v ++ oldval)

        oldval when set? and is_list(v) ->
          :lists.usort(v)

        _ ->
          v
      end

      List.keysort ukeymergerec(rest, List.keystore(b, k, 0, {k, v})), 0
    end
    @doc """
    Remove a item from proplist

    If called on flag, register or counter this has same effect as
    update
    """
    def reduce(vals, spec) do
      nil
    end

    @doc """
    Decodes a binary encoded proplist from riakc_obj

    If the object have siblings these will be recursively merged
    """
    def decode(obj, robject // true) do
      val = if robject do
          :riakc_obj.get_contents(obj)
        else
          obj
        end
      case val do
        [] ->
          []

        [{_,_}|_] = siblings ->
          siblings = lc {_md, v} inlist siblings do v end
          Enum.reduce siblings, [], fn(o, acc) ->
            ukeymergerec(binary_to_term(o), acc)
          end

        [_|_] = siblings ->
          Enum.reduce siblings, [], fn(o, acc) ->
            ukeymergerec(binary_to_term(o), acc)
          end
      end
    end
  end

  defmodule CRDT do
    @moduledoc """
    Bindings for future update of CRDT objects

    All functions in this module are sideeffect free and caller must
    save the data using the appropriate function in :riakc_pb_socket API.
    """

    @doc """
    Returns closure for updating a CRDT
    """
    def update(vals, spec) do
      case spec[:datatype] do
        :map ->      &update_map(vals,      &1)
        :set ->      &update_set(vals,      &1)
        :flag ->     &update_flag(vals,     &1)
        :register -> &update_register(vals, &1)
        :counter ->  &update_counter(vals,  &1)
      end
    end

    @doc """
    Returns closure to remove item from set or map CRDT

    If called on flag, register or counter this has same effect as
    update
    """
    def reduce(vals, spec) do
      case spec[:datatype] do
        :map ->      &reduce_map(vals,      &1)
        :set ->      &reduce_set(vals,      &1)
        :flag ->     &update_flag(vals,     &1)
        :register -> &update_register(vals, &1)
        :counter ->  &update_counter(vals,  &1)
      end
    end

    @doc """
    Decodes a CRDT into a nested-proplist
    """
    def decode(crdt) do
      case :riakc_datatype.module_for_term crdt do
        :undefined  ->
          {:error, {:unknown_crdt, crdt}}

        mod ->
          {:ok, mapper(mod, crdt)}
      end
    end


    defp reduce_set(vals, set) when is_list(vals), do:
      Enum.reduce(vals, set, &reduce_set/2)
    defp reduce_set(val, set), do:
      :riakc_set.del_element(val, set)

    defp update_set(vals, set) when is_list(vals), do:
      Enum.reduce(vals, set, &update_set/2)
    defp update_set(val, set), do:
      :riakc_set.add_element(val, set)

    defp update_flag(true,  flag),   do: :riakc_flag.enable flag
    defp update_flag(false, flag),   do: :riakc_flag.disable flag

    defp update_register(v, reg),    do: :riakc_register.set(v, reg)

    defp update_counter(n, counter), do: :riakc_counter.increment(n, counter)

    @doc """
    Updates a map datastructure with given proplist
    """
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

    defp mapper_red({{k, _}, v}) when is_list(v), do:
      {k, lc v0 inlist v do mapper_red(v0) end}
    defp mapper_red({{k, _}, v}), do:
      {k, v}
    defp mapper_red(v), do:
      v

    defp mapper(:riakc_map = m, crdt), do:
      m.fold(fn(k, v, acc) ->
        [mapper_red({k, v}) | acc]
      end, [], crdt)
    defp mapper(:riakc_set = m, crdt), do:
      :ordset.to_list m.value(crdt)
    defp mapper(mod, crdt), do:
      mod.value crdt
  end

end
