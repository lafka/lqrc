defmodule LQRC do

  require LQRC.Riak, as: Riak

  require LQRC.Domain, as: Domain

  def write(domain, sel, vals, opts // []) do
    spec = Domain.read domain
    Riak.write domain, sel, vals, spec, opts
  end

  def reduce(domain, sel, vals, opts // []) do
    spec = Domain.read domain

    Riak.reduce domain, sel, vals, spec, opts
  end

  def read(domain, sel, opts // []) do
    spec = Domain.read domain
    Riak.read domain, sel, spec, opts
  end

  def list(domain, sel), do: list(domain, sel, [])

  def list(domain, {:index, idx}, opts) do
    {:error, :notfound}
  end

  def list(domain, {:range, a, b}, opts) do
    {:error, :notfound}
  end

  def query(domain, q) do
    spec = Domain.read domain

    Riak.query domain, q, spec
  end
end
