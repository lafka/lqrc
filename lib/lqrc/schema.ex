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
  def match(schema, vals), do: match(schema, vals, [])
  def match(schema, vals, prefix) do
    defaultvals = Enum.reduce(schema, [], fn({k,v}, acc) ->
      case v[:default] do
        nil -> acc
        val ->
          merge(Enum.reverse(String.split(k, ".")) |> Enum.reduce(val, fn(nk, acc0) ->
            [{nk, acc0}]
          end), acc)
      end
    end)

    match(schema, merge(defaultvals, vals), Dict.keys(vals), prefix, [:ok, nil])
  end
  def match(schema, vals, [k|rest], prefix, [:ok, _prevk]) do
    sk = schemakeys(schema, prefix, k)

    match(schema, vals, rest, prefix,
      validate(vals[k], [k|prefix], sk, schema, vals))
  end
  def match(schema, vals, [k|rest], prefix, [{:ok, val}, prevk]) do
    vals = Dict.put vals, key(prevk, prefix), val
    sk = schemakeys(schema, prefix, k)

    match(schema, vals, rest, prefix,
      validate(vals[k], [k|prefix], sk, schema, vals))
  end
  def match(schema, vals, [k|rest], prefix, [:skip, prevk]) do
    vals = Dict.delete vals, key(prevk, prefix)
    sk = schemakeys(schema, prefix, k)

    match(schema, vals, rest, prefix,
      validate(vals[k], [k|prefix], sk, schema, vals))
  end
  def match(_schema, vals, [], prefix, [:ok, _prevk]), do: {:ok, vals}
  def match(_schema, vals, [], prefix, [{:ok, val}, prevk]), do:
    {:ok, Dict.put(vals, key(prevk, prefix), val)}
  def match(_schema, vals, [], prefix, [:skip, prevk]), do:
    {:ok, Dict.delete(vals, key(prevk, prefix))}
  def match(_schema, _vals, _keys, prefix, [{:error, err}, prevk]), do:
    {:error, [{key(prevk, prefix), err}]}

  def schemakeys(schema, prefix, k) do
    Enum.map [k, "*", key(k, "*")], &key(&1, prefix)
  end

  def key(k, prefix), do: Enum.join(Enum.reverse([k, prefix]), ".")

  defp merge(a, [{kb,vb}|restB]) do
    case Dict.fetch a, kb do
      {:ok, [{_,_}|_] = va} ->
        merge Dict.put(a, kb, merge(va, vb)), restB
      x ->
        merge Dict.put(a, kb, vb), restB
    end
  end
  defp merge(a, []), do: a

  defp validate(val, rkey, skeys, schema, acc) do
    case renderschema skeys, schema, rkey do
      {:ok, [sk, schema]} ->
        case schema[sk][:validator].(val, rkey, sk, schema, acc) do
          # Propagate  child errors
          [{:error, _}, _] = err ->
            err

          res ->
            [res, rkey]
        end

      {:error, _} = err ->
        err

      [{:error, _}, _rkey] = err ->
        err
    end
  end


  def renderschema(skeys, schema, rkey) do
    case Dict.take schema, skeys do
      [] -> [{:error, "unable to lookup schema: #{Enum.join(rkey, ", ")}"}, rkey]
      [{sk, _}|_] ->
        {:ok, [sk, Dict.merge(schema, [{sk, Dict.merge(schema[sk],
                                       [{:key, sk} | mapprops(schema[sk][:type])])} ] )]}
    end
  end

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
  defp mapprops(:integer),         do: [type: :string,
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
      case schema[sk][:regex] do
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
      max = schema[sk][:max]
      min = schema[sk][:min]

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

        {_, _buf} ->
          {:error, "not a valid integer"}
      end
    end
  end

  defmodule Validators.Enum do
    def valid?(val, rk, sk, schema, acc) do
      match = cond do
        is_binary(schema[sk][:match]) -> acc[schema[sk][:match]]
        true -> schema[sk][:match] end

      if (schema[sk][:map] || false) and match !== nil do
        match = Dict.keys match
      end

      case match !== nil and val in match do
        true -> :ok

        false when nil !== match ->
          {:error, "enum value must be one off #{Enum.join(match, ", ")}"}

        false ->
          {:error, "enum value matching against nil"}
      end
    end
  end

  defmodule Validators.Set do
    def valid?(val, rk, sk, schema, acc) when is_list(val) do
      case schema[sk][:itemvalidator] do
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
    def valid?(_val, rkey, _sk, _schema, _acc), do:
      [{:error, "key is not a set"}, rkey]
  end

  defmodule Validators.Map do
    def valid?(val, rkey,  sk, schema, acc), do: valid?(val, rkey, sk, schema, acc, [])
    def valid?([],  rkey, _sk, _schema, _acc, mapacc), do: {:ok, Enum.reverse(mapacc)}
    def valid?([{k, :null} | rest], rkey, sk, schema, acc, mapacc), do:
      valid?(rest, rkey, sk, schema, acc, mapacc)
    def valid?([{k, :undefined}|rest], rkey, sk, schema, acc, mapacc), do:
      valid?(rest, rkey, sk, schema, acc, mapacc)
    def valid?([{k, nil}|rest], rkey, sk, schema, acc, mapacc), do:
      valid?(rest, rkey, sk, schema, acc, mapacc)
    def valid?([{k,v}|rest], rkey, sk, schema, acc, mapacc) do
      subrkey = LQRC.Schema.key(k, rkey)
      subkeys = LQRC.Schema.schemakeys schema, sk, k

      case LQRC.Schema.renderschema subkeys, schema, [LQRC.Schema.key(k, sk)] do
        {:ok, [subkey, schema]} ->
          case schema[sk][:itemvalidator] || {schema[subkey][:validator], []} do
            {nil, _} ->
              valid? rest, rkey, sk, schema, acc, [{k, v} | mapacc]

            {validator, props} ->
              case validator.(v, subrkey, subkey, schema, acc) do
                :ok ->
                  valid? rest, rkey, sk, schema, acc, [{k, v} | mapacc]

                {:ok, v} ->
                  valid? rest, rkey, sk, schema, acc, [{k, v} | mapacc]

                :skip ->
                  valid? rest, rkey, sk, schema, acc, mapacc

                {:error, _} = err->
                  [err, subrkey]

                [{:error, _}, _] = err ->
                  err
              end
          end

        [{:error, _} = err, _rkey] ->
          [err, LQRC.Schema.key(k, sk)]
      end
    end
    def valid?([_|rest], rk, _sk, _schema, _acc, _mapacc) do
      [{:error, "none key/value in object"}, rk]
    end
  end

  defmodule Validators.Ignore do
    def valid?(val, _rk, sk, schema, _acc) do
      case schema[sk][:delete] do
        nil   -> :skip
        true  -> :skip
        false -> {:ok, val}
      end
    end
  end
end
