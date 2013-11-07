defmodule LQRC.Pooler do
  @moduledoc """
  Provide primitives to get and return pooler members from a group
  """

  defmacro __using__(opts) do
    group = opts[:group]
    quote do
      defp with_pid(fun, pid // nil, retfun // &return/2) do
        case pid || :pooler.take_group_member(unquote(group)) do
          p when is_pid(p) ->
            fun.(p) |> retfun.(p)

          err ->
              raise {:error, {:nopid, unquote(group)}}
        end
      end

      defp return(p), do: return(:ok, p)

      defp return({:ok, _} = r, p) do
        :pooler.return_group_member unquote(group), p, :ok; r end

      defp return(:ok, p) do
        :pooler.return_group_member unquote(group), p, :ok; :ok end

      defp return({:error, {:notfound, _}} = r, p) do
        :pooler.return_group_member unquote(group), p, :ok; r
        {:error, :notfound}
      end

      defp return({:error, :notfound} = r, p) do
        :pooler.return_group_member unquote(group), p, :ok; r
      end

      defp return({:error, _} = r, p) do
        :pooler.return_group_member unquote(group), p, :ok; r
      end
    end
  end
end
