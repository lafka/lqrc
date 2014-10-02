defmodule LQRC.ContentType do
  def encode(v, "application/x-erlang-binary"), do: :erlang.term_to_binary v
  def encode(v, "text/json"), do: :jsxn.encode v
  def encode(v, "application/json"), do: :jsxn.encode v
  def encode(v, "octet/stream"), do: v
  def encode(v, t) when is_list(t), do: encode(v, List.to_string(t))

  def decode(v, "application/x-erlang-binary"), do: :erlang.binary_to_term(v)
  def decode(v, "text/json"), do: :jsxn.decode v
  def decode(v, "application/json"), do: :jsxn.decode v
  def decode(v, "octet/stream"), do: v
  def decode("", :undefined), do: %{}
  def decode(v, t) when is_list(t), do: decode(v, List.to_string(t))
end
