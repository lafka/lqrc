defmodule LQRC.ContentType do
  def encode(v, "application/x-erlang-binary"), do: :erlang.term_to_binary v
  def encode(v, "text/json"), do: :jsxn.encode v
  def encode(v, "application/json"), do: :jsxn.encode v
  def encode(v, "octet/stream"), do: v
  def encode(v, t) when is_list(t), do: encode(v, :erlang.list_to_binary(t))

  def decode(v, "application/x-erlang-binary"), do: :erlang.binary_to_term(v)
  def decode(v, "text/json"), do: :jsxn.decode v
  def decode(v, "application/json"), do: :jsxn.decode v
  def decode(v, "octet/stream"), do: v
  def decode(v, t) when is_list(t), do: decode(v, :erlang.list_to_binary(t))
end
