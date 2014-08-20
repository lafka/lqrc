defmodule SchemaTest do
  use ExUnit.Case

  alias LQRC.Schema

#  require :riakc_pb_socket, as: PB
#  require LQRC.Pooler
#  LQRC.Pooler.__using__([group: :riak])
#
#  setup_all do
#    {:ok, {_container, host}} = LQRC.Test.Utils.Riak.maybe_start
#
#    {:ok, _pool} = :pooler.new_pool([{:name, :riak},
#      {:group, :riak},
#      {:max_count, 1000},
#      {:init_count, 10},
#      {:start_mfa, {:riakc_pb_socket,
#        :start_link,
#        [host, 8087]}}
#    ])
#
#    on_exit fn ->
#      :pooler.rm_pool(:riak)
#    end
#
#    :timer.sleep 1000
#
#    :ok
#  end

  @schema [
    {"default",            [type: :str, default: "val"]},
    {"nested", [type: :map]},
    {"nested.default", [type: :map]},
    {"nested.default.val", [type: :str, default: "myval"]},
    {"nested.default.val2", [type: :str, default: "yourval"]},
    {"str",       [type: :map]},
    {"str.id",    [type: :id]},
    {"str.resource", [type: :resource]},
    {"str.regex", [type: :regex, regex: ~r/^[a-zA-Z0-9]+$/]},
    {"str.enum",  [type: :enum, match: ["a", "b", "c"]]},
    {"int", [type: :map]},
    {"int.max",       [type: :int, max: 100]},
    {"int.max_neg",   [type: :int, max: -1]},
    {"int.min",       [type: :int, min: 1]},
    {"int.min_neg",   [type: :int, min: -100]},
    {"int.range",     [type: :int, min: 0, max: 10]},
    {"int.range_neg", [type: :int, min: -100, max: -10]},
    {"list", [type: :map]},
    {"list.hash", [type: :map]},
    {"list.hash.*", [type: :str]},
    {"list.id",   [type: :'list/id']},
    {"list.resource", [type: :'list/resource']},
  ]


  test "default" do
    m  = %{"default" => "val",
           "nested"  => %{
             "default" => %{
               "val2" => "yourval",
               "val"  => "myval"
             } } }
    m2 = %{"default" => "newval",
           "nested" => %{
             "default" => %{
               "val"  => "mynewval",
               "val2" => "yourval"
             } } }

    {:ok, mmatch}  = Schema.match @schema, []
    {:ok, m2match} = Schema.match @schema, [
      {"default", "newval"},
      {"nested", [{"default", [{"val", "mynewval"}]}]}
    ]

    assert m  == mmatch
    assert m2 == m2match

    assert :ok = Schema.valid? @schema, [{"str", [{"id", "abc"}]}]
    assert {:error, %{error: _, key: ["str", "id"]}} = Schema.match @schema, [
      {"str", [{"id", "!@#$%^&*"}]}]

    assert :ok = Schema.valid? @schema, [{"str", [{"resource", "abc/def"}]}]
    assert {:error, %{error: _, key: ["str", "resource"]}} = Schema.valid? @schema, [
      {"str", [{"resource", "!@#$%^&*"}]}]

    assert :ok = Schema.valid? @schema, [{"str", [{"regex", "azAZ09"}]}]
    assert {:error, %{error: _, key: ["str", "regex"]}} = Schema.valid? @schema, [
      {"str", [{"regex", "--azAZ09--"}]}]

    assert :ok = Schema.valid? @schema, [{"str", [{"enum", "a"}]}]
    assert {:error, %{error: _, key: ["str", "enum"]}} = Schema.valid? @schema, [
      {"str", [{"enum", "x"}]}]

    assert :ok = Schema.valid? @schema, [{"int", [{"max", 100}]}]
    assert {:error, %{error: _, key: ["int", "max"]}} = Schema.valid? @schema, [
      {"int", [{"max", 101}]}]

    assert :ok = Schema.valid? @schema, [{"int", [{"max_neg", -1}]}]
    assert {:error, %{error: _, key: ["int", "max_neg"]}} = Schema.valid? @schema, [
      {"int", [{"max_neg", 0}]}]

    assert :ok = Schema.valid? @schema, [{"int", [{"min", 1}]}]
    assert {:error, %{error: _, key: ["int", "min"]}} = Schema.valid? @schema, [
      {"int", [{"min", 0}]}]

    assert :ok = Schema.valid? @schema, [{"int", [{"min_neg", -100}]}]
    assert {:error, %{error: _, key: ["int", "min_neg"]}} = Schema.valid? @schema, [
      {"int", [{"min_neg", -101}]}]

    assert :ok = Schema.valid? @schema, [{"int", [{"range_neg", -10}]}]
    assert :ok = Schema.valid? @schema, [{"int", [{"range_neg", -100}]}]
    assert {:error, %{error: _, key: ["int", "range_neg"]}} = Schema.valid? @schema, [
      {"int", [{"range_neg", -101}]}]
    assert {:error, %{error: _, key: ["int", "range_neg"]}} = Schema.valid? @schema, [
      {"int", [{"range_neg", -9}]}]

    assert :ok = Schema.valid? @schema, [{"list", [{"hash", %{}}]}]
    assert :ok = Schema.valid? @schema, [{"list", [{"hash", [{"a", "b"}]}]}]
    assert :ok = Schema.valid? @schema, %{"list" => %{"hash" => %{"a" => "b"}}}
    assert {:error, %{error: _, key: ["list", "hash"]}} = Schema.valid? @schema, [
      {"list", [{"hash", [{"a", "b"}, 1]}]}]

    assert :ok = Schema.valid? @schema, [{"list", [{"id", []}]}]
    assert :ok = Schema.valid? @schema, [{"list", [{"id", ["a", "b"]}]}]
    assert {:error, %{error: _, key: ["list", "id"]}} = Schema.valid? @schema, [
      {"list", [{"id", ["a", {"key", "val"}]}]}]

    assert :ok = Schema.valid? @schema, [{"list", [{"resource", []}]}]
    assert :ok = Schema.valid? @schema, [
      {"list", [{"resource", ["a/b", "c/d", "e"]}]}]
    assert {:error, %{error: _, key: ["list", "resource"]}} = Schema.valid? @schema, [
      {"list", [{"resource", ["a/b", 1]}]}]

  end
end
