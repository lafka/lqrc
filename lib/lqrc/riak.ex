defmodule LQRC.Riak do

  require :riakc_pb_socket, as: PB
  require :riakc_obj, as: RObj

  require LQRC.ContentType, as: ContentType
  require LQRC.Pooler
  LQRC.Pooler.__using__([group: :riak])

  defmacro left :: right do
    quote do
      case unquote(left) do
        {:ok, res} -> res |> unquote(right)
        res -> res
      end
    end
  end

  # Macro to allow storing riak objects to filesystem for
  # compatability tests
  defmacrop write_test_data(spec, sel, obj) do
    project = Mix.Project.get!

    match? = quote do
      fn
        (_sel, nil) -> true
        (sel, match) when sel === match -> true
        (sel, match) ->
          case Enum.reverse match do
            [:_ | head] ->
              :lists.prefix Enum.reverse(head), sel

            _ ->
              false
          end
      end
    end

    if :test === Mix.env and function_exported?(project, :testdataset_path, 0) do
      path = project.testdataset_path
      patterns = case function_exported?(project, :testdataset_path, 0) do
        true -> project.testdataset_patterns
        false -> nil
      end

      quote do
        resource = case unquote(spec) do
          unquote(spec) when is_atom(unquote(spec)) -> [unquote(spec) | unquote(sel)]
          _ -> [unquote(spec)[:domain] | unquote(sel)]
        end
        encresource = integer_to_binary :binary.decode_unsigned(:erlang.term_to_binary resource), 36

        if Enum.any? unquote(patterns), &unquote(match?).(resource, &1) do
          File.mkdir_p! unquote(path)

          File.write! :filename.join(unquote(path), encresource), :erlang.term_to_binary unquote(obj)
        end
      end
    end
  end

  @doc """
  Write the content of `vals` into `sel`, possibly overwriting old
  data depending on bucket properties.

  @todo 2014-01-14; add support for rolling back partial fails
  """
  def write(spec, sel, vals, opts, obj \\ nil, oldvals \\ nil) do
    putopts = (opts[:putopts] || []) ++ (spec[:riak][:putopts] || [])
    opts = List.keystore opts, :putopts, 0, {:putopts, putopts}

    rollback = case obj do
      nil ->
        fn() -> delete spec, sel, opts end

      ^obj ->
        if oldvals do # re-fetch object and write back oldvals
          fn() -> update(spec, sel, oldvals, opts, nil) end
        else
          fn() -> {:error, "nothing to rollback to"} end
        end
    end

    ((({:ok, set_key(spec[:datatype], spec[:key], sel, vals)}
      |> dofun(spec, spec[:prewrite], sel))
      :: write_obj(spec, sel, obj, opts))
      :: decode spec)
      |> dofun_rollback(spec, spec[:postwrite], sel, [], rollback)
  end

  @doc """
  Write the content of `vals` into `sel` by merging it with previous data

  @todo 2014-01-14; add support for rolling back partial fails
  """
  def update(spec, sel, vals, opts, nil) do
    case read_obj spec, sel, opts do
      {:ok, obj} -> update spec, sel, vals, opts, obj
      err -> err
    end
  end

  def update(spec, sel, vals, opts, obj) do
      {md, oldvals} = decode_md obj
      obj  = if nil != md do RObj.update_metadata obj, md else obj end
      vals = ukeymergerec vals, oldvals

      putopts = (opts[:putopts] || []) ++ (spec[:riak][:putopts] || [])
      putopts = [:if_not_modified | putopts]
      opts = List.keystore opts, :putopts, 0, {:putopts, putopts}
      write spec, sel, vals, opts, obj, oldvals
  end

  @doc """
  Read the contents of `sel` and maybe issue a write request to
  resolve siblings
  """
  def read(spec, sel, opts) do
    passobj? = opts[:return_obj]
    case read_obj spec, sel, opts do
      {:ok, obj} ->
        case spec[:datatype] || decode_md obj do
          {nil, vals} when passobj? ->
            {:ok, vals, obj}

          {nil, vals} ->
            {:ok, vals}

          {_md, vals} when is_list(vals) ->
            opts = :lists.ukeymerge 1, opts, spec[:riak]
            # Update object to resolve sibling conflicts, update/5
            # uses :if_not_modified to try to ensure nothing gets out
            # of hand
            spawn fn() -> update spec, sel, [], opts, obj end
            cond do
              passobj? -> {:ok, vals, obj}
              true -> {:ok, vals}
            end

          type when passobj? ->
            {:ok, type.value(obj), obj}

          type ->
            {:ok, type.value obj}
        end

      err ->
        err
    end
  end

  @doc """
  Delete an item at `sel`

  @todo 2014-01-14; secure rollback on failed deletes
  """
  def delete(spec, sel, opts) do
    {bucket, key} = genkey spec, sel

    delopts = [{:dw, :one} | (opts[:delopts] || []) ++ (spec[:riak][:delopts] || [])]
    case dofun(:ok, spec, spec[:ondelete], sel) do
      {:ok, _} -> with_pid &PB.delete &1, bucket, key, delopts
      :ok -> with_pid &PB.delete &1, bucket, key, delopts
      err -> err
    end
  end

  @doc """
  Query SOLR for a document
  """
  def query(_spec, _q, _opts) do
    {:error, :notimplemented}
    #q = Enum.map q, fn({k,v}) -> "#{k}:#{v}" end
    #case with_pid(&PB.search &1, atom_to_binary(domain), q) do
    #  res ->
    #    IO.inspect res
    #    res
    #end
  end

  @doc """
  Get all items where value of index `List.last(sel)` is between a and b;
  specify bucket in the elements 0..n-1 in `sel`
  """
  def range(spec, sel, a, b, opts) do
    {bucket, idx} = genkey spec, sel

    idx = maybe_expand_idx idx, spec

    with_pid(&PB.get_index_range(&1, bucket, idx, a, b), opts[:pid]) |> indexret
  end

  @doc """
  Get all items where index `List.last(sel)` == `val`;
  specify bucket in the elements 0..n-1 in `sel`
  """
  def tagged(spec, sel, val, opts) do
    {bucket, idx} = genkey spec, sel

    idx = maybe_expand_idx idx, spec

    with_pid(&PB.get_index_eq(&1, bucket, idx, val), opts[:pid]) |> indexret
  end

  defp indexret({:ok, {:index_results_v1, res, _, _}}), do: {:ok, res}
  defp indexret(res), do: res

#  defp mapred() do
#    with_pid fun, opts[:pid], fn
#      ({:ok, [{0, vals}]}, pid) ->
#        return :ok, pid
#        decode vals vals
#  end

  @doc """
  Write `vals` to the selected object, if `object` equals nil a new
  riak object will be created

  Before writing, indexes will be appended to the document and `vals`
  will be encoded with content type defined in `spec`.
  """
  def write_obj(vals, spec, sel, nil, opts) do
    {bucket, key} = genkey spec, sel

    obj = RObj.new bucket, key, [], spec[:content_type]
    write_obj vals, spec, sel, obj, opts
  end
  def write_obj(vals, spec, sel, obj, opts) do
    matchedvals = case spec[:schema] do
      [_|_] = schema->
        LQRC.Schema.match schema, vals

      _ ->
        {:ok, vals}
    end

    case matchedvals do
      {:ok, vals} ->
        type = spec[:content_type] || "octet/stream"
        obj = RObj.update_value(
          update_obj_indexes(spec, RObj.update_content_type(obj, type), vals),
          ContentType.encode(vals, type))

        write_test_data(spec, sel, obj)

        opts = :lists.ukeymerge 1, opts, spec[:riak]

        case spec[:datatype] do
          nil -> with_pid &PB.put(&1, obj, opts[:putopts]), opts[:pid]
          type ->
            {bucket, key} = genkey spec, sel
            with_pid &PB.update_type(&1, bucket, key, type.to_op(vals), opts[:putopts]), opts[:pid]
        end

      {:error, _} = err -> err
    end
  end

  def read_obj(spec, sel, opts) do
    {bucket, key} = genkey spec, sel

    getopts = (opts[:getopts] || []) ++ (spec[:riak][:getopts] || [])
    if ! spec[:datatype] do
      with_pid &PB.get(&1, bucket, key, getopts), opts[:pid]
    else
      with_pid &PB.fetch_type(&1, bucket, key)
    end
  end

  defp decode(obj, _) when elem(obj, 0) == :riakc_obj do
    {_, val} = decode_md(obj)
    {:ok, val, obj}
  end
  defp decode(obj, spec), do:
    {:ok, spec[:datatype].value(obj), obj}

  defp decode_md(obj) do
    case RObj.value_count(obj) do
      1 ->
        vals = RObj.get_update_value obj
        {nil,
         ContentType.decode(vals, RObj.get_update_content_type(obj))}

      _ ->
        [{md,val}| contents] = RObj.get_contents obj
        acc = {md, ContentType.decode(val, RObj.md_ctype(md))}
        Enum.reduce contents, acc, fn({md, val}, {oldmd, acc}) ->
            t       = :dict.fetch("X-Riak-Last-Modified", oldmd)
            lastmod = :dict.fetch("X-Riak-Last-Modified", md)

            val = ContentType.decode(val, RObj.md_ctype(md))
            {oldmd, ukeymergerec(val, acc)}
          end
    end

  end

  @doc """
  Generate the riak {bucket, key} pair from `sel`
  """
  def genkey(spec, sel) when is_atom(spec), do:
    genkey(LQRC.Domain.read!(spec), sel)
  def genkey(spec, sel) do
    domain = atom_to_binary spec[:domain]

    {bucket, key} = case sel do
      [key] ->
        {domain, key}

      sel ->
        {a, [b]} = Enum.split sel, length(sel) - 1
        {Enum.join([domain | a], "/"), b}
    end

    case spec[:bucket_type] do
      nil  -> {bucket, key}
      type -> {{type, bucket}, key}
    end
  end
  defp set_key(_dt, nil, _sel, vals), do: vals
  defp set_key(nil, key, sel, vals), do:
    List.keystore(vals, key, 0, {key, List.last(sel)})
  defp set_key(_, _key,_sel, vals), do: vals


  defp dofun_rollback({:ok, acc, _obj}, spec, funs, sel, prepend, fun), do:
    dofun({:ok, acc}, spec, funs, sel, prepend, [fn() -> fun.() end])
  defp dofun_rollback(res, spec, funs, sel, prepend, _), do:
    dofun(res, spec, funs, sel, prepend, [])

  defp dofun(res, spec, funs, sel), do:
    dofun(res, spec, funs, sel, [])
  defp dofun(res, spec, funs, sel, prepend), do:
    dofun(res, spec, funs, sel, prepend, [])

  defp dofun(:ok = acc, spec, [{m, f, a}|rest], sel, prepend, rb) do
    apply(m, f, [spec, sel, acc | prepend ++ a]) |>
      dofun(spec, rest, sel, prepend, rb)
  end
  defp dofun({:ok, acc}, spec, [{m, f, a}|rest], sel, prepend, rb) do
    apply(m, f, [spec, sel, acc | prepend ++ a]) |>
      dofun(spec, rest, sel, prepend, rb)
  end
  defp dofun({:ok, acc, rbfun}, spec, [{m, f, a}|rest], sel, prepend, rb) do
    apply(m, f, [spec, sel, acc | prepend ++ a]) |>
      dofun(spec, rest, sel, prepend, [rbfun | rb])
  end

  defp dofun({:error, _} = err, spec, _, sel, _prepend, rollback) do
    {domain, k} = {spec[:domain], Enum.join(sel, "/")}
    length(rollback) > 0 and
      :error_logger.error_msg "transaction failed for #{domain} '#{k}': #{Kernel.inspect(err)}"

    lc f inlist rollback do
      case f.() do
        :ok -> :ok
        {:ok, _} -> :ok
        err ->
          :error_logger.error_msg "rollback failed for #{domain} '#{k}': #{Kernel.inspect(err)}"
      end
    end

    err
  end

  defp dofun(final, _spec, _funs, _sel, _prepend, _rb) do final end

  # Recursively merge the unsorted lists A and B, picking the last
  # value from list A if there are any duplicates.
  def ukeymergerec(a, []), do: a
  def ukeymergerec([], b), do: b
  def ukeymergerec([{k, v} | rest], b) do
    if nil === v or :undefined === v do
      ukeymergerec(rest, List.keydelete(b, k, 0))
    else
      v = case b[k] do
        oldval when is_list(v) and is_list(oldval) and is_tuple(hd(v)) ->
          ukeymergerec v, oldval

        _ ->
          v
      end

      List.keysort ukeymergerec(rest, List.keystore(b, k, 0, {k, v})), 0
    end
  end

  def maybe_expand_idx(idx, spec) when is_atom(spec), do:
    maybe_expand_idx(idx, LQRC.Domain.read!(spec))
  def maybe_expand_idx(idx, spec) do
    case List.keyfind spec[:index], idx, 1 do
      nil  -> idx
      {t, k} when is_list(k) ->
        {:ok, str} = String.to_char_list Enum.join(k, "/")
        {t, str}
      {t, k} ->
        {:ok, str} = String.to_char_list k
        {t, str}
    end
  end

  defp update_obj_indexes(spec, obj, vals) do
    case spec[:index] do
      [] ->
        obj

      [_|_] = indexes ->
        RObj.update_metadata obj,
          update_md_indexes(RObj.get_update_metadata(obj), vals, indexes)
    end
  end

  defp update_md_indexes(md, _vals, []), do: md
  defp update_md_indexes(md, vals, [idx|rest]), do:
    update_md_indexes(add_md_index(md, idx, vals), vals, rest)

  defp add_md_index(md, {idxtype, k}, vals) when is_list(k) do
    val = Enum.reduce k, "", fn(k, acc) -> Enum.join([acc, vals[k]], "/") end
    add_md_index2 md, {idxtype, Enum.join(k, "/")}, val
  end
  defp add_md_index(md, {_, k} = idx, vals), do:
    add_md_index2(md, idx, vals[k])

  defp add_md_index2(md, _idx, nil), do: md
  defp add_md_index2(md, idx, [_|_] = val), do:
    RObj.set_secondary_index(md, {idx, Enum.map(val, &map_hash_idx/1)})
  defp add_md_index2(md, idx, val), do:
    RObj.set_secondary_index(md, {idx, Enum.map([val], &map_hash_idx/1)})

  defp map_hash_idx({_, idx}), do: idx
  defp map_hash_idx(idx),      do: idx
end
