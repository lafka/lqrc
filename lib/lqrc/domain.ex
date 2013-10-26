defmodule LQRC.Domain do
  def read(domain) when is_atom(domain) do
    case :application.get_env :lqrc, domain do
      {:ok, spec} ->
        spec

      :undefined ->
        raise "unknown domain: #{domain}"
    end
  end

  def write(domain, props) when is_atom(domain) do
    :application.set_env :lqrc, domain, props
  end
end
