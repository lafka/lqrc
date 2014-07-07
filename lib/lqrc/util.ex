defmodule LQRC.Util do
  require LQRC.ContentType, as: ContentType

  def omit(_spec, _sel, vals, _keys) do
    {:ok, Enum.filter(vals, fn({k,_}) -> ! List.keymember? vals, k, 0 end)}
  end

  def pick(_spec, _sel, vals, _keys) do
    {:ok, Enum.filter(vals, fn({k,_}) -> List.keymember? vals, k, 0 end)}
  end

  def add_to_parent(spec, sel, acc, [{domain, key, idx}|rest]) do
    case add_to_parent2 domain,
                        Enum.slice(sel, 0, length(sel) -1),
                        {idx, [{acc[key], List.last(sel)}]}  do
      {:ok, _} -> add_to_parent spec, sel, acc, rest
      err -> err
    end
  end
  def add_to_parent(spec, sel, acc, [{domain, idx}|parents]) do
    case add_to_parent2 domain,
                        Enum.slice(sel, 0, length(sel) -1),
                        {idx, [List.last(sel)]}  do
      {:ok, _} -> add_to_parent spec, sel, acc, parents
      err -> err
    end
  end

  def add_to_parent(_spec, _sel, acc, []) do
    {:ok, acc}
  end

  def add_to_parent2(domain, sel, {idx, val}) do
    case LQRC.read domain, sel, [return_obj: true] do
      {:ok, vals, obj} ->
        val = Enum.uniq val ++ (vals[idx] || []) |> Enum.sort
        LQRC.update domain, sel, [{idx, val}], [], obj

      err ->
        err
    end
  end

  def decodeobj(spec, _sel, obj) do
    {:ok, LQRC.Riak.Obj.decode(obj, spec, true)}
  end

  def pushredq(spec, sel, vals), do:
    pushredq(spec, sel, vals, [])

  def pushredq(spec, [ns | sel], vals, opts) do
    out = ContentType.encode vals, (spec[:content_type] || "octet/stream")
    :redq.push [ns, spec[:domain] | sel], out, opts
  end
end
