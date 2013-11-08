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

  def range(domain, sel, a, b, opts // []) do
    spec = Domain.read domain
    Riak.range domain, sel, a, b, spec, opts
  end

  def index(domain, sel, val, opts // []) do
    spec = Domain.read domain
    Riak.index domain, sel, val, spec, opts
  end

  def query(domain, q) do
    spec = Domain.read domain

    Riak.query domain, q, spec
  end
end
