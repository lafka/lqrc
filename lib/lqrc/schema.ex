defmodule LQRC.Schema do


  defmodule Vals do
    def get(k, v) when is_atom(k), do: Dict.get(v, k)
    def get(k, v) when is_list(v), do: :proplists.get_value(k, v, nil)
    def get(k, v), do: Dict.get(v, k)
  end

  alias LQRC.Schema.Vals

  @doc """
  Check if `vals` are valid according to `schema`
  """
  def valid?(schema, vals, opts \\ []) do
    case match schema, vals, opts do
      {:ok, _} -> :ok
      res -> res
    end
  end

  @doc """
  Parse `vals` according to schema, filling in defaults where applicable
  """
  def match(schema, vals, opts \\ [], path \\ [], defaults \\ nil) do
    defaults = case {defaults, opts[:skip_defaults]} do
      {nil, true} -> %{}
      {nil, _} -> maybe_add_default_vals(schema, path, vals, opts)
      {defaults, _} -> defaults
    end

    ctx = matchctx schema, path, LQRC.Riak.ukeymergerec(vals, defaults, path)


    case Enum.reduce vals, ctx, &validatepair(&1, &2, opts) do
      %{error: nil, acc: acc} = ctx ->
        {:ok, getpath(acc, path)}

      %{error: {k, err}} ->
        {:error, %{:key => k, :error => err}}
    end
  rescue e in ArgumentError ->
    {:error, %{:key => Enum.reverse(e.message.key), :error => e.message.message}}
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

  defp filtermatch?(item, opts) do
    case {item[:filtermatch], opts[:skip_match_filter]} do
      {_, true} -> true
      {nil, _} -> true
      {items, _} ->
        Enum.all? items, fn({k, matchRules}) ->
          case opts[k] do
            nil ->
              true

            expect ->
              # positive matches are OR and negative matches are AND
              {neg, pos} = Enum.reduce matchRules, {nil, nil}, fn
                ({:!, match}, {nil, pos}) -> {match !== expect, pos}
                ({:!, match}, {neg, pos}) -> {neg and match !== expect, pos}
                (match, {neg, nil}) -> {neg, match === expect}
                (match, {neg, pos}) -> {neg, pos || match === expect}
              end

              neg || pos
          end
        end
    end
  end

  defp maybe_add_default_vals(schema, [], acc, opts) do
    Enum.reduce(schema, acc, fn({k,v}, acc) ->
      # @todo; olav; 2014-08-20 - allow wildcard defaults to augment collections
      #parts = String.split(k, ".") |> Enum.reduce(0, fn
      #  ("*", acc) -> [Enum.at(path, length(acc)) | acc]
      #  (p, acc) -> [p | acc]
      #end) |> Enum.reverse
      #prefix? = :lists.prefix path, parts

      match? = filtermatch?(v, opts)
      case v[:default] do
        val when val !== nil and match? ->
          val = if is_function(val) do
            val.()
          else
            val
          end

          default = Enum.reverse(String.split(k, ".")) |>
            Enum.reduce(val, fn(nk, acc0) ->
              Dict.put %{}, nk, acc0
            end)
          LQRC.Riak.ukeymergerec acc, default
        _ -> acc
      end
    end)
  end
  defp maybe_add_default_vals(_schema, _path), do: %{}

  defp validatepair({k, v}, %{error: nil, schema: schema, acc: acc, path: path} = ctx, opts) do
    itempath = path ++ [k]

    case findkey itempath, Dict.keys(schema) do
      [schemakey | _] ->
        props = Vals.get schemakey, schema

        match? = filtermatch?(props, opts)

        case (props[:validator].(v, itempath, schemakey, schema, acc, opts)) do
          :ok when match? ->
            %{ctx | :acc => updatepath(acc, itempath, v)}

          {:ok, newval} when match? ->
            %{ctx | :acc => updatepath(acc, itempath, newval)}

          {:ok, _} ->
            %{ctx | :acc => deletepath(acc, itempath)}

          :ok ->
            %{ctx | :acc => deletepath(acc, itempath)}

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
  defp validatepair({k, v}, %{error: err} = ctx, _opts) when err !== nil, do: ctx

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

  def deletepath(acc, [key]), do: Dict.delete(acc, key)
  def deletepath(acc, [h | rest]) do
    case get(acc, h) do
      nil -> acc
      sub -> Dict.put(acc, h, deletepath(sub, rest))
    end
  end

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
                                        validator: &__MODULE__.Validators.String.valid?/6,
                                        regex: ~r/^[a-zA-Z0-9+-]+$/]
  defp mapprops(:resource),        do: [type: :string,
                                        validator: &__MODULE__.Validators.String.valid?/6,
                                        regex: ~r/^[a-zA-Z0-9-+\/]+$/]
  defp mapprops(:string),          do: [type: :string,
                                        validator: &__MODULE__.Validators.String.valid?/6]
  defp mapprops(:integer),         do: [type: :integer,
                                        validator: &__MODULE__.Validators.Integer.valid?/6]
  defp mapprops(:float),           do: [type: :float,
                                        validator: &__MODULE__.Validators.Float.valid?/6]
  defp mapprops(:enum),            do: [type: :enum,
                                        validator: &__MODULE__.Validators.Enum.valid?/6]
  defp mapprops(:set),             do: [type: :set,
                                        validator: &__MODULE__.Validators.Set.valid?/6]
  defp mapprops(:'list/id'),       do: [type: :set,
                                        validator: &__MODULE__.Validators.Set.valid?/6,
                                        itemvalidator: {
                                          &__MODULE__.Validators.String.valid?/6,
                                          mapprops(:id)}]
  defp mapprops(:'list/resource'), do:  [type: :set,
                                        validator: &__MODULE__.Validators.Set.valid?/6,
                                        itemvalidator: {
                                          &__MODULE__.Validators.String.valid?/6,
                                          mapprops(:resource)}]
  defp mapprops(:map),             do: [type: :map,
                                        validator: &__MODULE__.Validators.Map.valid?/6]
  defp mapprops(:'list/hash'),     do: [type: :map,
                                        validator: &__MODULE__.Validators.Map.valid?/6]
  defp mapprops(:ignore),          do: [type: :ignore,
                                        validator: &__MODULE__.Validators.Ignore.valid?/6]

  defmodule Validators.String do
    alias LQRC.Schema.Vals

    def valid?(val, _rkey, sk, schema, _acc, _opts) do
      case Vals.get(sk, schema)[:regex] do
        _ when not is_binary(val) -> {:error, "not a string"}
        nil -> {:ok, val}
        {Regex, _, regex, _, _} ->
          {:ok, regex} = Regex.compile regex
          cond do
            Regex.match? regex, val -> {:ok, val}
            true -> {:error, "invalid string format"}
          end

        regex ->
          cond do
            Regex.match? regex, val -> {:ok, val}
            true -> {:error, "invalid string format"}
          end
      end
    end
  end

  defmodule Validators.Integer do
    alias LQRC.Schema.Vals

    def valid?(val, _rkey, sk, schema, _acc, _opts) when is_integer(val) do
      max = Vals.get(sk, schema)[:max]
      min = Vals.get(sk, schema)[:min]

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
    def valid?(val, rkey, sk, schema, acc, opts) when is_binary(val) do
      case Integer.parse(val) do
        {val, ""} ->
          valid?(val, rkey, sk, schema, acc, opts)

        error ->
          {:error, "not a valid integer"}
      end
    end
  end

  defmodule Validators.Float do
    def valid?(val, _rkey, sk, schema, _acc, _opts) when is_float(val) do
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
    def valid?(val, rkey, sk, schema, acc, opts) when is_binary(val) do
      case Float.parse(val) do
        {val, ""} ->
          valid?(val, rkey, sk, schema, acc, opts)

        error ->
          {:error, "not a valid float"}
      end
    end
    def valid?(val, rkey, sk, schema, acc, opts) do
      if true === opts[:acceptint] and is_integer(val) do
        valid?(val + 0.0, rkey, sk, schema, acc, opts)
      else
        {:error, "not a valid float"}
      end
    end
  end

  defmodule Validators.Enum do
    alias LQRC.Schema.Vals

    def valid?(val, rk, sk, schema, acc, opts) do
      match = cond do
        is_binary(matchkey = Vals.get(sk, schema)[:match]) ->
          LQRC.Schema.getpath acc, String.split(matchkey, ".")

        true ->
          Vals.get(sk, schema)[:match]
      end

      match = if (Vals.get(sk, schema)[:map] || false) and match !== nil do
        Dict.keys match
      else
        match
      end

      case (match !== nil and val in match) || true === opts[:partial] do
        true -> {:ok, val}

        false when nil !== match ->
          {:error, "enum value must be one off #{Enum.join(match, ", ")}"}

        false ->
          {:error, "enum value matching against nil"}
      end
    end
  end

  defmodule Validators.Set do
    alias LQRC.Schema.Vals

    def valid?(val, rk, sk, schema, acc, opts) when is_list(val) do
      case Vals.get(sk, schema)[:itemvalidator] do
        nil ->
          :ok

        {nil, _} ->
          :ok

        {validator, props} ->
          pos = Enum.find_index val, fn(v) ->
            case validator.(v, rk, props, schema, acc, opts) do
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
    def valid?(_val, _rkey, _sk, _schema, _acc, _opts), do:
      {:error, "key is not a set"}
  end

  defmodule Validators.Map do
    alias LQRC.Schema.Vals

    def valid?(%{} = vals, rk, sk, schema, acc, opts) when map_size(vals) > 0 do
      case LQRC.Schema.match(schema, vals, opts, rk, acc) do
        {:ok, nil} -> :skip
        res -> res
      end
    end
    def valid?(%{} = vals, rk, sk, schema, acc, opts) when map_size(vals) === 0 do
      case Vals.get(sk, schema)[:default] do
        [] -> {:ok, %{}}
        _ ->
          case LQRC.Schema.match(schema, vals, opts, rk, acc) do
            {:ok, nil} -> :skip
            res -> res
          end
      end
    end
    def valid?([{_,_} | _] = vals, rk, sk, schema, acc, opts) do
      conv(vals, &valid?(&1, rk, sk, schema, acc, opts))
    end
    def valid?(nil, rk, sk, schema, acc, opts) do
      {:ok, nil}
    end
    def valid?([_|_], _rk, _sk, _schema, _acc, _opts), do:
      {:error, "all map items must be a k/v pair"}
    def valid?([], _rk, _sk, _schema, _acc, _opts), do:
      {:ok, %{}}
    def valid?(val, _rk, _sk, _schema, _acc, _opts), do:
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
    alias LQRC.Schema.Vals

    def valid?(val, _rk, sk, schema, _acc, _opts) do
      case Vals.get(sk, schema)[:delete] do
        nil   -> :skip
        true  -> :skip
        false -> {:ok, val}
      end
    end
  end
end
