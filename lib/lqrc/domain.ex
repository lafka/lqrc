defmodule LQRC.Domain do
  @moduledoc """
  Provides ability to query domain information
  """

  @doc """
  Read given domain and returns proplist with information

  The following items will be available:
    datatype:      nil | atom()  %% The riak datatype to use - not implemented
    searchable:    boolean()     %% If item should be indexed by yokozuna - not implemented
    merge_updates: boolean()     %% Flag to merge writes, only for nil datatypes
    sub:           [domain()]    %% List of sub domains - not implemented
    parent:        domain()      %% The parent domain - not implemented
    index:         [index()]     %% List of 2i indicies to store - not implemented
  """
  def read(domain) when is_atom(domain) do
    case :application.get_env :lqrc, domain do
      {:ok, spec} ->
        spec

      :undefined ->
        raise "unknown domain: #{domain}"
    end
  end

  def write(domain, props) when is_atom(domain) do
    match = [:datatype, :searchable, :merge_updates, :sub, :parent, :index]
    case Enum.reduce match, Keyword.keys(props), fn(a, b) -> b -- [a] end do
      [] ->
        :application.set_env :lqrc, domain, props

      errs ->
        {:error, {:invalid_keys, errs}}
    end
  end
end
