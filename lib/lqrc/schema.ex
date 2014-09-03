defmodule LQRC.Schema do
  @doc """
  Check if `vals` are valid according to `schema`
  """
  def valid?(schema, vals) do
    case match schema, vals do
      {:ok, _} -> :ok
      res -> res
    end
  end

  @doc """
  Parse `vals` according to schema, filling in defaults where applicable
  """
  def match(schema, vals, path \\ [], defaults \\ nil) do
    ctx = matchctx schema, path, defaults || maybe_add_default_vals(schema, path, vals)

    case Enum.reduce vals, ctx, &validatepair/2 do
      %{error: nil, acc: acc} = ctx ->
        {:ok, getpath(acc, path)}

      %{error: {k, err}} ->
        {:error, %{:key => k, :error => err}}
    end
  end

  defp matchctx(schema, path, vals) do
    schema = Enum.map schema, fn({k, props}) ->
      {k, :lists.ukeymerge(1, props, mapprops(props[:type]))}
    end

    %{  schema: schema,
        acc: vals,       # The result of schema matches
        path: path,      # Keep track of position in the tree
        error: nil
    }
  end

  defp maybe_add_default_vals(schema, [], acc) do
    Enum.reduce(schema, acc, fn({k,v}, acc) ->
      # @todo; olav; 2014-08-20 - allow wildcard defaults to augment collections
      #parts = String.split(k, ".") |> Enum.reduce(0, fn
      #  ("*", acc) -> [Enum.at(path, length(acc)) | acc]
      #  (p, acc) -> [p | acc]
      #end) |> Enum.reverse
      #prefix? = :lists.prefix path, parts

      case v[:default] do
        nil -> acc
        val ->
          default = Enum.reverse(String.split(k, ".")) |>
            Enum.reduce(val, fn(nk, acc0) -> Dict.put %{}, nk, acc0 end)
          LQRC.Riak.ukeymergerec acc, default
      end
    end)
  end
  defp maybe_add_default_vals(_schema, _path), do: %{}

  defp validatepair({k, v}, %{error: nil, schema: schema, acc: acc, path: path} = ctx) do
    itempath = path ++ [k]

    case findkey itempath, Dict.keys(schema) do
      [schemakey | _] ->
        props = :proplists.get_value schemakey, schema

        case (props[:validator].(v, itempath, schemakey, schema, acc)) do
          :ok ->
            %{ctx | :acc => updatepath(acc, itempath, v)}

          {:ok, newval} ->
            %{ctx | :acc => updatepath(acc, itempath, newval)}

          :skip ->
            %{ctx | :acc => deletepath(acc, itempath)}

          {:error, err} when is_map(err) ->
            %{ctx | :error => {err[:key], err[:error]}}

          {:error, err} ->
            %{ctx | :error => {itempath, err}}
        end

      [] ->
        %{ctx | :error => {:noschema, path ++ [k]}}
    end
  end
  defp validatepair({k, v}, %{error: err} = ctx) when err !== nil, do: ctx

  defp findkey(path, keys) do
    key = Enum.join path, "."

    rx = (for k <- path, do: "(?:#{k}|\\*)\\.") ++ ["?$"]
    {:ok, rx} = Regex.compile Enum.join ["^" | rx]

    Enum.filter keys, &Regex.match?(rx, &1)
  end

  # @todo fix tail recursion???
  def updatepath([], [h | rest], val) when length(rest) > 0, do:
    Dict.put(%{}, h, updatepath(%{}, rest, val))
  def updatepath([_|_] = acc, path, val), do:
    updatepath(Enum.into(acc, %{}), path, val)
  def updatepath([], [key], val), do:
    Dict.put(%{}, key, val)
  def updatepath(acc, [key], val), do:
    Dict.put(acc, key, val)
  def updatepath(%{} = acc, [h | rest], val) do
    Dict.put(acc, h, updatepath(get(acc, h) || %{}, rest, val))
  end

  def deletepath(nil, _), do: nil
  def deletepath(acc, [key]), do: Dict.delete(acc, key)
  def deletepath(acc, [h | rest]), do:
    Dict.put(acc, h, deletepath(get(acc, h), rest))

  def getpath(nil, _), do: nil
  def getpath(acc, path) when is_list(acc), do: getpath(Enum.into(acc, %{}), path)
  def getpath(%{} = acc, []), do: acc
  def getpath(%{} = acc, [key]), do: acc[key]
  def getpath(%{} = acc, [h | rest]), do: getpath(get(acc, h), rest)

  defp get(%{} = acc, k), do: acc[k]
  defp get([], _), do: nil
  defp get([_|_] = acc, k), do: :proplists.get_value(acc, k, nil)

  defp mapprops(:str),             do: mapprops(:string)
  defp mapprops(:regex),           do: mapprops(:string)
  defp mapprops(:int),             do: mapprops(:integer)
  defp mapprops(:id),              do: [type: :string,
                                        validator: &__MODULE__.Validators.String.valid?/5,
                                        regex: ~r/^[a-zA-Z0-9+-]+$/]
  defp mapprops(:resource),        do: [type: :string,
                                        validator: &__MODULE__.Validators.String.valid?/5,
                                        regex: ~r/^[a-zA-Z0-9-+\/]+$/]
  defp mapprops(:string),          do: [type: :string,
                                        validator: &__MODULE__.Validators.String.valid?/5]
  defp mapprops(:integer),         do: [type: :integer,
                                        validator: &__MODULE__.Validators.Integer.valid?/5]
  defp mapprops(:enum),            do: [type: :enum,
                                        validator: &__MODULE__.Validators.Enum.valid?/5]
  defp mapprops(:set),             do: [type: :set,
                                        validator: &__MODULE__.Validators.Set.valid?/5]
  defp mapprops(:'list/id'),       do: [type: :set,
                                        validator: &__MODULE__.Validators.Set.valid?/5,
                                        itemvalidator: {
                                          &__MODULE__.Validators.String.valid?/5,
                                          mapprops(:id)}]
  defp mapprops(:'list/resource'), do:  [type: :set,
                                        validator: &__MODULE__.Validators.Set.valid?/5,
                                        itemvalidator: {
                                          &__MODULE__.Validators.String.valid?/5,
                                          mapprops(:resource)}]
  defp mapprops(:map),             do: [type: :map,
                                        validator: &__MODULE__.Validators.Map.valid?/5]
  defp mapprops(:'list/hash'),     do: [type: :map,
                                        validator: &__MODULE__.Validators.Map.valid?/5]
  defp mapprops(:ignore),          do: [type: :ignore,
                                        validator: &__MODULE__.Validators.Ignore.valid?/5]

  defmodule Validators.String do
    def valid?(val, _rkey, sk, schema, _acc) do
      case :proplists.get_value(sk, schema, nil)[:regex] do
        _ when not is_binary(val) -> {:error, "not a string"}
        nil -> {:ok, val}
        regex ->
          cond do
            Regex.match? regex, val -> {:ok, val}
            true -> {:error, "invalid string format"}
          end
      end
    end
  end

  defmodule Validators.Integer do
    def valid?(val, _rkey, sk, schema, _acc) when is_integer(val) do
      max = :proplists.get_value(sk, schema, nil)[:max]
      min = :proplists.get_value(sk, schema, nil)[:min]

      cond do
        nil == max and nil == min ->
          :ok

        is_integer(max) and is_integer(min) and val < min and val > max ->
          {:error, "value must be in range #{min}..#{max}"}

        is_integer(max) and val > max ->
          {:error, "value must smaller than #{max}"}

        is_integer(min) and val < min ->
          {:error, "value must greater than #{min}"}

        true ->
          :ok
      end
    end
    def valid?(val, rkey, sk, schema, acc) when is_binary(val) do
      case Integer.parse(val) do
        {val, ""} ->
          valid?(val, rkey, sk, schema, acc)

        error ->
          {:error, "not a valid integer"}
      end
    end
  end

  defmodule Validators.Enum do
    def valid?(val, rk, sk, schema, acc) do
      match = cond do
        is_binary(matchkey = :proplists.get_value(sk, schema, nil)[:match]) ->
          LQRC.Schema.getpath acc, String.split(matchkey, ".")

        true ->
          :proplists.get_value(sk, schema, nil)[:match]
      end

      match = if (:proplists.get_value(sk, schema, nil)[:map] || false) and match !== nil do
        Dict.keys match
      else
        match
      end

      case match !== nil and val in match do
        true -> {:ok, val}

        false when nil !== match ->
          {:error, "enum value must be one off #{Enum.join(match, ", ")}"}

        false ->
          {:error, "enum value matching against nil"}
      end
    end
  end

  defmodule Validators.Set do
    def valid?(val, rk, sk, schema, acc) when is_list(val) do
      case :proplists.get_value(sk, schema, nil)[:itemvalidator] do
        {nil, _} ->
          :ok

        {validator, props} ->
          pos = Enum.find_index val, fn(v) ->
            case validator.(v, rk, props, schema, acc) do
              {:ok, _} -> false
              :ok      -> false
              _        -> true
            end
          end

          cond do
            nil !== pos -> {:error, "element #{pos} is not a valid #{props[:type]}"}
            true        -> :ok
          end
      end
    end
    def valid?(_val, _rkey, _sk, _schema, _acc), do:
      {:error, "key is not a set"}
  end

  defmodule Validators.Map do
    def valid?(%{} = vals, rk, sk, schema, acc) when map_size(vals) > 0 do
      LQRC.Schema.match(schema, vals, rk, acc)
    end
    def valid?(%{} = vals, rk, sk, schema, acc) when map_size(vals) === 0 do
      case :proplists.get_value(sk, schema)[:default] do
        [] -> {:ok, %{}}
        _ -> LQRC.Schema.match(schema, vals, rk, acc)
      end
    end
    def valid?([{_,_} | _] = vals, rk, sk, schema, acc) do
      conv(vals, &LQRC.Schema.match(schema, &1, rk, acc))
    end
    def valid?([_|_], _rk, _sk, _schema, _acc), do:
      {:error, "all map items must be a k/v pair"}
    def valid?([], _rk, _sk, _schema, _acc), do:
      {:ok, %{}}
    def valid?(val, _rk, _sk, _schema, _acc), do:
      {:error, "expected item of type 'map'"}

    defp conv(vals, csp), do:
      conv(vals, csp, %{})
    defp conv([], csp, acc), do:
      csp.(acc)
    defp conv([{k,v}|rest], csp, acc), do:
      conv(rest, csp, Map.put(acc, k, v))
    defp conv([v|rest], csp, acc), do:
      {:error, "all map items must be a k/v pair"}
  end

  defmodule Validators.Ignore do
    def valid?(val, _rk, sk, schema, _acc) do
      case :proplists.get_value(sk, schema, nil)[:delete] do
        nil   -> :skip
        true  -> :skip
        false -> {:ok, val}
      end
    end
  end
end
