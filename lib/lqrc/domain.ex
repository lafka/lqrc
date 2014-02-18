defmodule LQRC.Domain do
  @moduledoc """
  Provides ability to query domain information
  """

  @defaults [
    domain: nil,    # Domain key as atom
    key: "key",     # Primary key of domain
    datatype: nil,  # Any type specifics for the given
                    # backend (e.g. CRDT type for Riak)
    bucket_type: nil, # The actual bucket type
    sub: [],        # Any domain located underneath
    parent: nil,    # The parent domain
    index: [],      # list of secondary indexes to add
    schema: [],     # Schema to validate against
    prewrite: [],   # Prewrite hooks:  [{m, f, [_|_] || []}, ..]
    postwrite: [],  # Postwrite hooks: [{m, f, [_|_] || []}, ..]
    ondelete: [],   # Delete/rollback hooks: [{m, f, [_|_] || []}, ..]
    riak: [         # Options passed to riak
      putopts: [],
      getopts: [],
      delopts: []
    ],
    content_type:  "application/json"
  ]

  # The LQRC domain used to store domains
  defmacro lqrc do
    quote do Keyword.merge @defaults, [
      domain: :__lqrc,
      key: nil,
      content_type: "application/x-erlang-binary"] end
  end

  @doc """
  Read given domain and returns proplist with information
  """
  def read(domain) when is_atom(domain) do
    case :ets.lookup :domains, domain do
      [{^domain, spec}] -> {:ok, spec}
      [] ->
        case LQRC.read lqrc, ["domains", atom_to_binary domain] do
          {:ok, spec} = res ->
            true = :ets.insert :domains, {domain, spec}
            res

          {:error, _} = err ->
            err
        end
    end
  end
  def read!(domain) when is_atom(domain) do
    case read(domain) do
      {:ok, spec} ->
        spec

      {:error, :notfound} ->
        raise "unknown domain: #{domain}"
    end
  end

  def write(domain, props) when is_atom(domain) do
    match = Keyword.keys @defaults

    props = Keyword.merge @defaults, List.keystore(props, :domain, 0, {:domain, domain})

    case Enum.reduce match, Keyword.keys(props), fn(a, b) -> b -- [a] end do
      [] ->
        :ets.delete :domains, domain
        LQRC.write lqrc, ["domains", atom_to_binary domain], props

      errs ->
        {:error, {:invalid_keys, errs}}
    end
  end
end
