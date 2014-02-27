defmodule LqrcTest do
  use ExUnit.Case

  require :riakc_pb_socket, as: PB
  require LQRC.Pooler
  LQRC.Pooler.__using__([group: :riak])

  @user  [key: "email", content_type: "text/json", riak: [putopts: [:return_body]]]
  @user2 [key: "email", content_type: "application/x-erlang-binary", riak: [putopts: [:return_body]]]
  @tdom [riak: [putopts: [:return_body]]]

  defmacro left :: right do
    quote do
      case unquote(left) do
        {:ok, res} -> res |> unquote(right)
        res -> res
      end
    end
  end

  setup do
    :ok = RH.reset
  end

  test "domain creation" do
    :ok = LQRC.Domain.write :user, @user
    assert {:ok, domain} = LQRC.Domain.read :user
    assert domain[:key] == "email"
  end

  test "write resource" do
    :ok = LQRC.Domain.write :user, @user
    assert {:ok, [{"email", "dev@nyx"}]} == LQRC.write :user, ["dev@nyx"], []
  end

  test "read resource" do
    :ok = LQRC.Domain.write :user, @user
    assert {:ok, [{"email", "dev@nyx"}]} == LQRC.write :user, ["dev@nyx"], []
    assert {:ok, [{"email", "dev@nyx"}]} == LQRC.read  :user, ["dev@nyx"], []
  end

  test "merge resource update" do
    :ok = LQRC.Domain.write :t, @tdom
    assert {:error, :notfound} == LQRC.read :t, ["k"]
    assert {:error, :notfound} == LQRC.update :t, ["k"], [{"val", "-"}]

    assert {:ok, newvals} = LQRC.write :t, ["k"], [{"val", "a"}]
    assert newvals["val"]  == "a"

    {:ok, newvals} = LQRC.update :t, ["k"], [{"val", "x"},
                                             {"val2", "b"}],
                                  [riak: [getopts: [:return_body]]]
    assert newvals["val"]  == "x"
    assert newvals["val2"] == "b"
  end

  test "delete resource" do
    :ok = LQRC.Domain.write :user, @user
    {:ok, _} = LQRC.write :user, ["dev@nyx"], []
    :ok = LQRC.delete :user, ["dev@nyx"], []
  end

  test "switch resource content-type" do
    :ok = LQRC.Domain.write :user, @user
    {:ok, _} = LQRC.write :user, ["dev@nyx"], []
    :ok = LQRC.Domain.write :user, @user2
    LQRC.Domain.read! :user
    assert {:ok, [{"email", "dev@nyx"}]} = LQRC.read :user, ["dev@nyx"]
    assert {:ok, _} = LQRC.write :user, ["dev@nyx"], [a: "b"]
  end

  test "sibling merge" do
    :ok = LQRC.Domain.write :tsm, @tdom
    {:ok, _} = LQRC.write :tsm, ["k"], [{"val1", "a"}]
    {:ok, _} = LQRC.write :tsm, ["k"], [{"val2", "b"}]
    {:ok, vals} = LQRC.write :tsm, ["k"], [{"val3", "c"}]
    assert {:ok, vals} == LQRC.read :tsm, ["k"], [{"val3", "c"}]
  end

  test "fail update on divergent resource" do
    :ok = LQRC.Domain.write :tdiv, @tdom
    spec = LQRC.Domain.read! :tdiv
    {:ok, _} = LQRC.write :tdiv, ["k"], [{"val1", "a"}]
    {:ok, obj} = LQRC.Riak.read_obj spec, ["k"], []
    assert {:ok, _} = LQRC.update :tdiv, ["k"], [{"val1", "b"}]
    assert {:error, "modified"} == LQRC.Riak.update spec, ["k"], [{"val1", "c"}], [], obj
  end

  test "get indexed query: range/tagged" do
    :ok = LQRC.Domain.write :rq, []
    keys = lc x inlist :lists.seq 33, 126 do "a" <> <<x>> end

    lc x inlist keys do
      :ok = LQRC.write :rq, [x], []
    end

    assert {:ok, _keys} = LQRC.range(:rq, ["$key"], "a!", "a~") :: Enum.sort
    assert {:ok, ["as"]} = LQRC.tagged(:rq, ["$key"], "as")
  end

  test "CRDT counters" do
    :ok = with_pid &PB.set_bucket_type(&1, "counter", [{:datatype, :counter}, {:active, true}])
    :ok = LQRC.Domain.write :counter, [bucket_type: "counter",
                                 datatype: :riakc_counter,
                                 content_type: "octet/stream"]

    :ok = LQRC.write :counter, ["networks"], :riakc_counter.increment(:riakc_counter.new)
    {:ok, 1} = LQRC.read :counter, ["networks"]
    :ok = LQRC.write :counter, ["networks"], :riakc_counter.increment(:riakc_counter.new)
    {:ok, 2} = LQRC.read :counter, ["networks"]
    :ok = LQRC.write :counter, ["networks"], :riakc_counter.increment(8, :riakc_counter.new)
    {:ok, 10} = LQRC.read :counter, ["networks"]
  end

  test "CRDT set" do
    :ok = with_pid &PB.set_bucket_type(&1, "set", [{:datatype, :set}, {:active, true}])
    :ok = LQRC.Domain.write :set, [bucket_type: "set",
                             datatype: :riakc_set,
                             content_type: "octet/stream"]

    :ok = LQRC.write :set, ["networks"], :riakc_set.add_element("a", :riakc_set.new)
    {:ok, ["a"]} = LQRC.read :set, ["networks"]
    :ok = LQRC.write :set, ["networks"], :riakc_set.add_element("b", :riakc_set.new)
    {:ok, ["a", "b"]} = LQRC.read :set, ["networks"]
    :ok = LQRC.write :set, ["networks"], :riakc_set.del_element("b", :riakc_set.new)
    {:ok, ["a"]} = LQRC.read :set, ["networks"]
  end

  test "ensure parent relations:set" do
    LQRC.Domain.write :p1, []
    LQRC.Domain.write :c1, [parent: [:p1],
      postwrite: [{LQRC.Util, :add_to_parent, [[{:p1, "children"}]]}]]

    :ok = LQRC.write :p1, ["parent"], []
    :ok = LQRC.write :c1, ["parent", "child-1"], []
    :ok = LQRC.write :c1, ["parent", "child-2"], []

    assert {:ok, [{"children", ["child-1", "child-2"]}, {"key", "parent"}]} ==
      LQRC.read :p1, ["parent"]
  end

  test "ensure parent relations:hash" do
    LQRC.Domain.write :p2, []
    LQRC.Domain.write :c2, [parent: [:p2], riak: [putopts: [:return_body]],
      postwrite: [{LQRC.Util, :add_to_parent, [[{:p2, "idx", "children"}]]}]]

    :ok = LQRC.write :p2, ["parent"], []
    :ok = LQRC.write :c2, ["parent", "child-1"], [{"idx", "1"}]
    :ok = LQRC.write :c2, ["parent", "child-2"], [{"idx", "2"}]

    assert {:ok, [{"children", [{"1", "child-1"}, {"2", "child-2"}]}, {"key", "parent"}]} ==
     LQRC.read :p2, ["parent"]
  end

  test "domain schema" do
    :ok = LQRC.Domain.write :schema, [key: nil, schema: [
      {"default",            [type: :str, default: "val"]},
      {"nested.default.val", [type: :str, default: "myval"]},
      {"str.id",    [type: :id]},
      {"str.resource", [type: :resource]},
      {"str.regex", [type: :regex, regex: %r/^[a-zA-Z0-9]+$/]},
      {"str.enum",  [type: :enum, match: ["a", "b", "c"]]},
      {"int.max",       [type: :int, max: 100]},
      {"int.max_neg",   [type: :int, max: -1]},
      {"int.min",       [type: :int, min: 1]},
      {"int.min_neg",   [type: :int, min: -100]},
      {"int.range",     [type: :int, min: 0, max: 10]},
      {"int.range_neg", [type: :int, min: -100, max: -10]},
      {"list.hash.*", [type: :str]},
      {"list.id",   [type: :'list/id']},
      {"list.resource", [type: :'list/resource']},
    ]]

    defopts = [putopts: [:return_body]]
    m  = [{"default", "val"}, {"nested", [{"default", [{"val", "myval"}]}]}]
    m2 = [{"default", "newval"}, {"nested", [{"default", [{"val", "myval"}]}]}]
    assert {:ok, m}  == LQRC.write :schema, ["default"], [], defopts
    assert {:ok, m2} == LQRC.write :schema, ["default"], [{"default", "newval"}], defopts

    assert :ok = LQRC.write :schema, ["str.id"], [
      {"str", [{"id", "abc"}]}]
    assert {:error, [[{"key", "str.id"},_]]} = LQRC.write :schema, ["str.id"], [
      {"str", [{"id", "!@#$%^&*"}]}]

    assert :ok = LQRC.write :schema, ["str.resource"], [
      {"str", [{"resource", "abc/def"}]}]
    assert {:error, [[{"key", "str.resource"},_]]} = LQRC.write :schema, ["str.resource"], [
      {"str", [{"resource", "!@#$%^&*"}]}]

    assert :ok = LQRC.write :schema, ["str.regex"], [
      {"str", [{"regex", "azAZ09"}]}]
    assert {:error, [[{"key", "str.regex"},_]]}  = LQRC.write :schema, ["str.regex"], [
      {"str", [{"regex", "--azAZ09--"}]}]

    assert :ok = LQRC.write :schema, ["str.enum"], [
      {"str", [{"enum", "a"}]}]
    assert {:error, [[{"key", "str.enum"},_]]}  = LQRC.write :schema, ["str.enum"], [
      {"str", [{"enum", "x"}]}]

    assert :ok = LQRC.write :schema, ["int.max"], [
      {"int", [{"max", 100}]}]
    assert {:error, [[{"key", "int.max"},_]]}  = LQRC.write :schema, ["str.max"], [
      {"int", [{"max", 101}]}]

    assert :ok = LQRC.write :schema, ["int.max_neg"], [
      {"int", [{"max_neg", -1}]}]
    assert {:error, [[{"key", "int.max_neg"},_]]}  = LQRC.write :schema, ["str.max_neg"], [
      {"int", [{"max_neg", 0}]}]

    assert :ok = LQRC.write :schema, ["int.min"], [
      {"int", [{"min", 1}]}]
    assert {:error, [[{"key", "int.min"},_]]}  = LQRC.write :schema, ["str.min"], [
      {"int", [{"min", 0}]}]

    assert :ok = LQRC.write :schema, ["int.min_neg"], [
      {"int", [{"min_neg", -100}]}]
    assert {:error, [[{"key", "int.min_neg"},_]]}  = LQRC.write :schema, ["str.min_neg"], [
      {"int", [{"min_neg", -101}]}]

    assert :ok = LQRC.write :schema, ["int.range_neg"], [
      {"int", [{"range_neg", -10}]}]
    assert :ok = LQRC.write :schema, ["int.range_neg"], [
      {"int", [{"range_neg", -100}]}]
    assert {:error, [[{"key", "int.range_neg"},_]]}  = LQRC.write :schema, ["str.range_neg"], [
      {"int", [{"range_neg", -101}]}]
    assert {:error, [[{"key", "int.range_neg"},_]]}  = LQRC.write :schema, ["str.range_neg"], [
      {"int", [{"range_neg", -9}]}]

    assert :ok = LQRC.write :schema, ["list/hash"], [
      {"list", [{"hash", []}]}]
    assert :ok = LQRC.write :schema, ["list/hash"], [
      {"list", [{"hash", [{"a", "b"}]}]}]
    assert {:error, [[{"key", "list.hash"},_]]} = LQRC.write :schema, ["list/hash"], [
      {"list", [{"hash", [{"a", "b"}, 1]}]}]

    assert :ok = LQRC.write :schema, ["list/id"], [
      {"list", [{"id", []}]}]
    assert :ok = LQRC.write :schema, ["list/id"], [
      {"list", [{"id", ["a", "b"]}]}]
    assert {:error, [[{"key", "list.id"},_]]} = LQRC.write :schema, ["list/id"], [
      {"list", [{"id", ["a", 1]}]}]

    assert :ok = LQRC.write :schema, ["list/resource"], [
      {"list", [{"resource", []}]}]
    assert :ok = LQRC.write :schema, ["list/resource"], [
      {"list", [{"resource", ["a/b", "c/d", "e"]}]}]
    assert {:error, [[{"key", "list.resource"},_]]} = LQRC.write :schema, ["list/resource"], [
      {"list", [{"resource", ["a/b", 1]}]}]
  end

  test "schema wildcard parent" do
    :ok = LQRC.Domain.write :wildcardschema, [key: nil, schema: [
      {"list", [type: :'list/hash']},
      {"list.*.wildcard", [type: :'list/resource']}
    ]]

    defopts = [putopts: [:return_body]]
    match = [{"list", [{"a", [{"wildcard", ["x", "y", "z"]}]}]}]
    assert {:ok, match} == LQRC.write :wildcardschema, ["nested_wildcard"], match, defopts
    assert {:error, [[{"key", "list.a.wildcard"},_]]} = LQRC.write :wildcardschema, ["nested_wildcard"], [
      {"list", [{"a", [{"wildcard", "x"}]}]}]
  end
end
