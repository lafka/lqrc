defmodule LQRC.Collection do

  use Bitwise

  @moduledoc """
  Some tasks to for reducing collections to a minimum it's taken for
  granted that the collections are sorted. A collection can be either
  a map or a set

  This is a very basic utility that only works on binary keys, in the
  future adding keycomposites would allow for much richer interface
  using only mapreduce

  Here are some example using ISO dates:
  # Get the date for the 1st day of all months
  keyslice({:mask, <<255,255,255,255,"-",255,255,"-","01">>}, vals)
  # => [{"2013-08-01T13:24:41.314159Z", ....}, # {"2013-08-01T...",..},..}

  # Get a range
  keyslice({:range, "2013-08", "2013-09"}, vals)
  # => [{"2013-08-01T13:24:41.314159Z", ....}, # {"2013-08-01T...",..},..}

  # A neat trick we can do is to combine masks and ranges for instance
  # to query the first week of august from 2000 to 2012.  We use multiple
  # masks to first filter on month, then a not-mask to filter out
  # invalid dates. The not-masks matches on ascii the representation,
  # hence 77 would be any character NOT in the range 1-7

  opts = ['and-mask': <<255,255,255,255,"-08-0">>,
          'not-mask': <<0,0,0,0,0,0,0,0,0,77>>]
  keyslice {:range, "2000", "2012"}, vals, opts
  # => [{"2000-08-01T13:24:41.314159Z", ....}, # {"2000-08-01T...",..},..}

  """

  def keyslice(q, vals), do: keyslice(q, vals, [])

  @doc """
  Slices through all keys applying the mask

  Options:
   :or-mask     A additional OR mask to apply
   :and-mask    A additional AND mask to apply
   :not-mask    A additional NOT mask to apply
   :mask-type   The default type of mask, one of :or/:and/:not
  """
  # @todo clever way to only perform match without converting bin<>int
  def keyslice({:mask, mask}, vals, opts) when is_binary(mask) do
    fun = fn(0) -> 0; (_) -> 255 end

    matchmask = :binary.decode_unsigned bc <<x>> inbits mask, do: <<(fun.(x))>>
    realmask   = :binary.decode_unsigned mask

    keyslice({:mask, realmask, matchmask, size(mask)}, vals, opts, 0)
  end

  def keyslice({:mask, _mask, _mmask, _size}, [], _opts, _), do: {:ok, []}
  def keyslice({:mask, _mask, _mmask, _size}, vals, _opts, p)
      when p === length(vals), do:
    {:ok, vals}
  def keyslice({:mask, _m, _mm, size} = q, [{k, _} | rest], opts, p)
      when size > size(k), do:
    keyslice(q, rest, opts, p)
  def keyslice({:mask, mask, mmask, size} = q, [{k, _} = v | rest], opts, p) do
    kdec = band(:binary.decode_unsigned(String.slice(k, 0, size)), mmask)

    cond do
      kdec === band(mask, kdec) ->
        vals = Enum.reverse([v | Enum.reverse(rest)])
        keyslice q, vals, opts, p + 1

      true ->
        keyslice q, rest, opts, p
    end
  end

  # Reduce each side of collection until range is met
  def keyslice({:range, a, b}, vals, opts), do: 
    keyslice({:range, a, b}, vals, opts, :left)

  def keyslice({:range, a, b}, [{k, _} | rest], opts, :left) when k < a, do:
    keyslice({:range, a, b}, rest, opts, :left)

  def keyslice({:range, a, b}, vals, opts, :left), do:
    keyslice({:range, a, b}, Enum.reverse(vals), opts, :right)

  def keyslice({:range, a, b}, [{k, _} | rest], opts, :right) when k > b, do:
    keyslice({:range, a, b}, rest, opts, :right)

  def keyslice({:range, a, b}, vals, opts, :right), do:
    {:ok, Enum.reverse(vals)}


  # Reduces the collection to keys matching prefix

  # The algorithm is simple, reduce from left side until match is made
  # then reduce from right side.
  def keyslice({:prefix, p}, vals, opts) do
    ps = size(p)
    fun = fn
      (fun, [{<<^p :: [binary, size(ps)], _ :: binary>>, _} = r | _] = m, :left) ->
        fun.(fun, Enum.reverse(m), :right)

      (_, [{<<^p :: [binary, size(ps)], _ :: binary>>, _} | _] = m, :right) ->
        {:ok, Enum.reverse(m)}

      (fun, [_ | rest], :left) ->
        fun.(fun, rest, :left)

      (fun, [_ | rest], :right) ->
        fun.(fun, rest, :right)
    end

    fun.(fun, vals, :left)
  end
end
