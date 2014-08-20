defmodule LQRC.Collection do

  use Bitwise

  @moduledoc """
  Some tasks to for reducing collections to a minimum it's taken for
  granted that the collections are sorted. A collection can be either
  a map or a set

  This is a very basic utility that only works on binary keys, in the
  future adding keycomposites would allow for much richer interface
  using only mapreduce


  # Get a range
  keyslice({:range, "2013-08", "2013-09"}, vals)
  # => [{"2013-08-01T13:24:41.314159Z", ....}, # {"2013-08-01T...",..},..}
  """

  def keyslice(q, vals), do: keyslice(q, vals, [])

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
    ps = byte_size p
    fun = fn
      (fun, [{<<p2 :: binary-size(ps), _>>, _} = r | _] = m, :left) when p2 === p ->
        fun.(fun, Enum.reverse(m), :right)

      (_, [{<<p2 :: binary-size(ps), _ :: binary>>, _} | _] = m, :right) when p2 === p->
        {:ok, Enum.reverse(m)}

      (fun, [_ | rest], :left) ->
        fun.(fun, rest, :left)

      (fun, [_ | rest], :right) ->
        fun.(fun, rest, :right)
    end

    fun.(fun, vals, :left)
  end
end
