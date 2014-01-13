defmodule LQRC do

  require LQRC.Domain, as: Domain

  @backend LQRC.Riak

  def write(domain, sel, vals, opts // []) do
    call(domain, :write, [sel, vals, opts])
  end

  def update(domain, sel, vals, opts // [], obj // nil), do:
    call(domain, :update, [sel, vals, opts, obj])

  def read(domain, sel, opts // []), do:
    call(domain, :read, [sel, opts])

  def delete(domain, sel, opts // []), do:
    call(domain, :delete, [sel, opts])

  def range(domain, sel, a, b, opts // []), do:
    call(domain, :range, [sel, a, b, opts])

  def tagged(domain, sel, val, opts // []), do:
    call(domain, :tagged, [sel, val, opts])

  def query(domain, q, opts // []), do:
    call(domain, :query, [q, opts])

  defp call(domain, fun, args) when is_atom(domain) do
    call(Domain.read!(domain), fun, args)
  end
  defp call(spec, fun, args) do
    argc = length(args) + 1

   :erlang.fun_info &LQRC.Riak.write/4
    case function_exported? @backend, fun, argc do
      true ->
        Kernel.apply LQRC.Riak, fun, [spec | args]

      false ->
        {:error, {:inaccessible_fun, {@backend, fun, argc}}}
    end
  end
end
