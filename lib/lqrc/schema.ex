defmodule LQRC.Schema do
  @doc """
  Check if `vals` are valid according to `schema`
  """
  def validate(schema, vals) do
    case match schema, vals do
      {:ok, _} -> :ok
      res -> res
    end
  end

  @doc """
  Parse `vals` according to schema, filling in defaults where applicable

  Undefined parent elements of nested keys will have be implicitly
  typed as list/hash values.
  """
  def match(schema, vals),         do: match(schema, vals, "")
  def match(schema, vals, prefix) do
    vals = Enum.reduce schema, vals, fn({k, props}, acc) ->
      cond do
        nil === props[:default] -> acc
        true -> fill_tree acc, String.split(k, "."), props[:default]
      end
    end

    match([{"_", [type: :'list/hash']}|schema], vals, prefix, [], [])
  end
  def match(_schema, [], _prefix, [], acc),  do: {:ok, acc}
  def match(_schema, [], _prefix, errs, _), do: {:error, errs}
  def match(schema, [{rk, v}|rest], prefix, errs, acc)  do
    nested? = is_list(v) and length(v) > 0 and is_tuple(hd(v)) or true

    pickkey(schema, rk, prefix, nested?) |>
      match2(schema, {rk, v}, rest, prefix, errs, acc)
  end

  def match2({:ok, k}, schema, {rk, v}, rest, prefix, errs, acc) do
    case parse(schema[k][:type], schema[k], k, v) do
      :ok when is_list(v) ->
        case match schema, v, mkey(rk, prefix), [], [] do
          {:ok, v} ->
            match schema, rest, prefix, errs, List.keystore(acc, rk, 0, {rk, v})

          {:error, [err]} ->
            match schema, rest, prefix, [err | errs], acc
        end

      :ok ->
        match schema, rest, prefix, errs, List.keystore(acc, rk, 0, {rk, v})

      :continue ->
        match schema, rest, prefix, errs, List.keystore(acc, rk, 0, {rk, v})

      {:error, err} ->
        match schema, rest, prefix, [expanderr(mkey(rk, prefix), err) | errs], acc
      end
  end
  def match2({:error, err}, schema, {k,_}, rest, prefix, errs, acc) do
    match(schema, rest, prefix, [expanderr(k, err) | errs], acc)
  end

  def mkey(key, ""), do: key
  def mkey(key, prefix), do: prefix <> "." <> key

  defp fill_tree(acc, [], _v), do: acc
  defp fill_tree(acc, [k], v) do
    case acc[k] do
      nil -> [{k, v} | acc]
      _ -> acc
    end
  end
  defp fill_tree(acc, [k|rest], v), do:
    List.keystore(acc, k, 0, {k, fill_tree((acc[k] || []), rest, v)})

  defp pickkey(schema, k, prefix, nested?) do
    cond do
      nil !== schema[k = mkey(k, prefix)   ] -> {:ok, k}
      nil !== schema[k2 = mkey("*", prefix)] -> {:ok, k2}
      nested? -> {:ok, "_"}
      true ->
        newk = mkey(k, prefix)
        {:error, "cannot validate '#{newk}', not in schema"}
    end
  end

  defp parse(:str, _props, _k, <<_ :: binary>>), do: :ok

  defp parse(:id, _props, k, <<id :: binary>>) do
    case Regex.match? %r/^[a-zA-Z0-9-]+$/, id do
      true  -> :ok
      false -> {:error, "value of '#{k}' not a valid id"}
    end
  end

  defp parse(:resource, _props, k, <<id :: binary>>) do
    case Regex.match? %r/^[a-zA-Z0-9-\/@.]+$/, id do
      true  -> :ok
      false -> {:error, "value of '#{k}' not a valid resource"}
    end
  end

  defp parse(:regex, props, k, <<id :: binary>>) do
    case Regex.match? props[:regex], id do
      true  -> :ok
      false -> {:error, "value of '#{k}' is not a valid id"}
    end
  end

  defp parse(:enum, props, _k, <<val :: binary>>) do
    case Enum.member? props[:match], val do
      true  -> :ok
      false ->
        vals = "'#{Enum.join(props[:match], "', '")}'"
        {:error, "value of '#{val}' must be one of #{vals}"}
    end
  end

  defp parse(:int, props, k, val) when is_integer(val) do
    {min, max} = {props[:min], props[:max]}
    cond do
      nil !== min and nil !== max ->
        expandbool val >= min and val <= max,
          {:error, "#{k} = #{val} must be in the range #{min}..#{max}"}

      nil !== max ->
        expandbool val <= max,
          {:error, "#{k} = #{val} must be less than or equal to #{max}"}

      nil !== min ->
        expandbool val >= min,
          {:error, "#{k} = #{val} must be great than or equal to #{min}"}

      true ->
        :ok
    end
  end
  defp parse(:int, _props, k, _val), do:
    {:error, "value of '#{k}' is not a integer"}

  defp parse(:'list/hash', props, _k, []) do
    case props[:deep] do
      false -> :continue
      _ -> :ok
    end
  end
  defp parse(:'list/hash' = t, props, k, [{_,_}|rest]), do:
    parse(t, props, k, rest)
  defp parse(:'list/hash', _props, _k, [e|_]), do:
    {:error, "element '#{e}' not a key/value pair"}
  defp parse(:'list/hash', _props, _k, _), do:
    {:error, "not a key/value list"}

  defp parse(:'list/id', _props, _k, []), do:
    :continue
  defp parse(:'list/id' = t, props, k, [<<e :: binary>>|rest]) do
    case Regex.match? %r/^[a-zA-Z0-9-]+$/, e do
        true -> parse(t, props, k, rest)
        false -> {:error, "element '#{e}' not a valid id"}
    end
  end
  defp parse(:'list/id', _props, _k, [e|_]), do:
    {:error, "element '#{e}' not a valid id"}

  defp parse(:'list/resource', _props, _k, []), do:
    :continue
  defp parse(:'list/resource' = t, props, k, [<<e :: binary>>|rest]) do
    case Regex.match? %r/^[a-zA-Z0-9-\/@.]+$/, e do
        true -> parse(t, props, k, rest)
        false -> {:error, "element '#{e}' not a valid resource"}
    end
  end
  defp parse(:'list/resource', _props, _k, [e|_]), do:
    {:error, "element '#{e}' not a valid resource"}

  defp parse(type, _props, k, _val), do:
    {:error, "error", "invalid type '#{type}' for '#{k}'"}

  defp expandbool(:true, _), do: :ok
  defp expandbool(:false, err), do: err

  defp expanderr(k, err), do: [{"key", k}, {"error", err}]
end
