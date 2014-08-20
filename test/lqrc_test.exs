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

  setup_all do
    {:ok, {_container, host}} = LQRC.Test.Utils.Riak.maybe_start

    {:ok, _pool} = :pooler.new_pool([{:name, :riak},
      {:group, :riak},
      {:max_count, 1000},
      {:init_count, 10},
      {:start_mfa, {:riakc_pb_socket,
        :start_link,
        [host, 8087]}}
    ])

    on_exit fn ->
      :pooler.rm_pool(:riak)
    end

    :timer.sleep 1000

    :ok
  end

  test "domain creation" do
    :ok = LQRC.Domain.write :user, @user
    assert {:ok, domain} = LQRC.Domain.read :user
    assert domain[:key] == "email"
  end

  test "write resource" do
    :ok = LQRC.Domain.write :user, @user
    assert {:ok, %{"email" => "dev@nyx"}} == LQRC.write :user, ["dev@nyx"], []
  end

  test "read resource" do
    :ok = LQRC.Domain.write :user, @user
    assert {:ok, %{"email" => "dev@nyx"}} == LQRC.write :user, ["dev@nyx"], []
    assert {:ok, %{"email" => "dev@nyx"}} == LQRC.read  :user, ["dev@nyx"]
  end

  test "merge resource update" do
    :ok = LQRC.Domain.write :t, @tdom

    k = genkey

    assert {:error, :notfound} == LQRC.read :t, [k]
    assert {:error, :notfound} == LQRC.update :t, [k], %{"val" => "-"}

    assert {:ok, newvals} = LQRC.write :t, [k], %{"val" => "a"}
    assert newvals["val"]  == "a"

    {:ok, newvals} = LQRC.update :t, [k], %{"val"  => "x",
                                            "val2" => "b"},
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
    assert {:ok, %{"email" => "dev@nyx"}} = LQRC.read :user, ["dev@nyx"]
    assert {:ok, _} = LQRC.write :user, ["dev@nyx"], [a: "b"]
  end

  test "sibling merge" do
    :ok = LQRC.Domain.write :tsm, @tdom
    {:ok, _} = LQRC.write :tsm, ["k"], %{"val1" => "a"}
    {:ok, _} = LQRC.write :tsm, ["k"], [{"val2", "b"}]
    {:ok, vals} = LQRC.write :tsm, ["k"], [{"val3", "c"}]
    assert {:ok, vals} == LQRC.read :tsm, ["k"]
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
    for x <- 33..126, do: LQRC.write(:rq, ["a" <> <<x>>], [])

    assert {:ok, _keys} = LQRC.range(:rq, ["$key"], "a!", "a~") :: Enum.sort
    assert {:ok, ["as"]} = LQRC.tagged(:rq, ["$key"], "as")
  end

  test "CRDT counters" do
    :ok = with_pid &PB.set_bucket_type(&1, "counter", [{:datatype, :counter}, {:active, true}])
    :ok = LQRC.Domain.write :counter, [bucket_type: "counter",
                                 datatype: :riakc_counter,
                                 content_type: "octet/stream"]


    k = genkey

    :ok = LQRC.write :counter, [k], :riakc_counter.increment(:riakc_counter.new)
    {:ok, 1} = LQRC.read :counter, [k]
    :ok = LQRC.write :counter, [k], :riakc_counter.increment(:riakc_counter.new)
    {:ok, 2} = LQRC.read :counter, [k]
    :ok = LQRC.write :counter, [k], :riakc_counter.increment(8, :riakc_counter.new)
    {:ok, 10} = LQRC.read :counter, [k]
  end

  test "CRDT set" do
    :ok = with_pid &PB.set_bucket_type(&1, "set", [{:datatype, :set}, {:active, true}])
    :ok = LQRC.Domain.write :set, [bucket_type: "set",
                             datatype: :riakc_set,
                             content_type: "octet/stream"]

    k = genkey

    :ok = LQRC.write :set, [k], :riakc_set.add_element("a", :riakc_set.new)
    {:ok, ["a"]} = LQRC.read :set, [k]
    :ok = LQRC.write :set, [k], :riakc_set.add_element("b", :riakc_set.new)
    {:ok, ["a", "b"], ctx} = LQRC.read :set, [k], return_obj: true
    :ok = LQRC.write :set, [k], :riakc_set.del_element("b", ctx)
    {:ok, ["a"]} = LQRC.read :set, [k]
  end

  test "ensure parent relations:set" do
    LQRC.Domain.write :p1, []
    LQRC.Domain.write :c1, [parent: [:p1],
      postwrite: [{LQRC.Util, :add_to_parent, [[{:p1, "children"}]]}]]

    [parent, childA, childB] = [genkey, genkey, genkey]

    :ok = LQRC.write :p1, [parent], []
    :ok = LQRC.write :c1, [parent, childA], []
    :ok = LQRC.write :c1, [parent, childB], []

    assert {:ok, %{"children" => Enum.sort([childA, childB]), "key" => parent}} ==
      LQRC.read :p1, [parent]
  end

  test "ensure parent relations:hash" do
    LQRC.Domain.write :p2, []
    LQRC.Domain.write :c2, [parent: [:p2], riak: [putopts: [:return_body]],
      postwrite: [{LQRC.Util, :add_to_parent, [[{:p2, "idx", "children"}]]}]]

    [parent, childA, childB] = [genkey, genkey, genkey]

    :ok = LQRC.write :p2, [parent], []
    :ok = LQRC.write :c2, [parent, childA], [{"idx", "1"}]
    :ok = LQRC.write :c2, [parent, childB], [{"idx", "2"}]

    assert {:ok, %{"children" => %{"1" => childA, "2" => childB}, "key" => parent}} ==
     LQRC.read :p2, [parent]
  end

  test "domain schema" do
    :ok = LQRC.Domain.write :schema, [key: nil, schema: [
      {"str",       [type: :map]},
      {"str.id",    [type: :id]},
      {"list", [type: :map]},
      {"list.hash", [type: :map]},
      {"list.hash.*", [type: :str]},
      {"list.id",   [type: :'list/id']},
      {"list.resource", [type: :'list/resource']},
    ]]

    defopts = [putopts: [:return_body]]
    assert :ok = LQRC.write :schema, ["str.id"], [
      {"str", [{"id", "abc"}]}]
    assert {:error, %{key: ["str", "id"]}} = LQRC.write :schema, ["str.id"], [
      {"str", [{"id", "!@#$%^&*"}]}], defopts

    assert :ok = LQRC.write :schema, ["list/hash"], [
      {"list", [{"hash", %{}}]}]
    assert :ok = LQRC.write :schema, ["list/hash"], [
      {"list", [{"hash", [{"a", "b"}]}]}]
    assert {:error, %{key: ["list", "hash"]}} = LQRC.write :schema, ["list/hash"], [
      {"list", [{"hash", [{"a", "b"}, 1]}]}]

    assert :ok = LQRC.write :schema, ["list/id"], [
      {"list", [{"id", []}]}]
    assert :ok = LQRC.write :schema, ["list/id"], [
      {"list", [{"id", ["a", "b"]}]}]
    assert {:error, %{key: ["list", "id"]}} = LQRC.write :schema, ["list/id"], [
      {"list", [{"id", ["a", {"key", "val"}]}]}]

    assert :ok = LQRC.write :schema, ["list/resource"], [
      {"list", [{"resource", []}]}]
    assert :ok = LQRC.write :schema, ["list/resource"], [
      {"list", [{"resource", ["a/b", "c/d", "e"]}]}]
    assert {:error, %{key: ["list", "resource"]}} = LQRC.write :schema, ["list/resource"], [
      {"list", [{"resource", ["a/b", 1]}]}]
  end

  test "schema wildcard parent" do
    :ok = LQRC.Domain.write :wildcardschema, [key: nil, schema: [
      {"list", [type: :'list/hash']},
      {"list.*", [type: :'list/hash']},
      {"list.*.wildcard", [type: :'list/resource']}
    ]]

    defopts = [putopts: [:return_body]]
    match = %{"list" => %{"a" => %{"wildcard" => ["x", "y", "z"]}}}
    assert {:ok, match} == LQRC.write :wildcardschema, ["nested_wildcard"], match, defopts
    assert {:error, %{key: ["list","a","wildcard"]}} = LQRC.write :wildcardschema, ["nested_wildcard"], [
      {"list", [{"a", [{"wildcard", "x"}]}]}], defopts
  end

  test "schema :ignore type (ro-keys / invalidated keys)" do
    :ok = LQRC.Domain.write :ignoretype, [key: nil, schema: [
      {"key", [type: :str]},
      {"meta", [type: :ignore]},
    ]]

    assert {:ok, %{"key" => "val"}} == LQRC.write :ignoretype, ["k"], [
      {"key", "val"},
      {"meta", 1234567890}
    ], [putopts: [:return_body]]

    :ok = LQRC.Domain.write :ignore_no_delete, [key: nil, schema: [
      {"key", [type: :str]},
      {"term", [type: :ignore, delete: false]},
      {"meta", [type: :map]},
      {"meta.*", [type: :map]},
      {"meta.*.*", [type: :ignore, delete: false]}
    ]]

    payload = %{
      "key"  => "val",
      "term" => "qazQAZ",
      "meta" => %{
        "a" => %{
          "str" => "y",
          "map" => %{"x" => "y"},
        } } }

    assert {:ok, payload} ==
      LQRC.write :ignore_no_delete, ["k"], payload, [putopts: [:return_body]]
  end

  test "schema delete nil values" do
    :ok = LQRC.Domain.write :nilvals, [key: nil, schema: [
      {"a", [type: :map]},
      {"a.*", [type: :map]},
      {"a.*.*", [type: :string]},
    ]]

    :ok = LQRC.write  :nilvals, ["x"], [{"a", [{"b", [{"c", "x"}, {"d", "y"}]}]}]
    :ok = LQRC.update :nilvals, ["x"], [{"a", [{"b", nil}]}]
    assert {:ok, %{"a" => %{}}} == LQRC.read :nilvals, ["x"]
  end

  test "schema enum as key in map" do
    :ok = LQRC.Domain.write :memberschema, [key: nil, schema: [
      {"hmap", [type: :map]},
      {"hmap.*", [type: :string]},
      {"k", [type: :enum, match: "hmap", map: true]}
    ]]

    :ok = LQRC.write :memberschema, ["x"], [{"hmap", [{"a", "x"}, {"b", "y"}]}]
    map = %{"a" => "x", "b" => "y"}

    defopts = [putopts: [:return_body]]

    assert {:ok, %{"hmap" => map, "k" => "a"}} ==
      LQRC.update :memberschema, ["x"], [{"k", "a"}], defopts

    assert {:ok, %{"hmap" => map, "k" => "b"}} ==
      LQRC.update :memberschema, ["x"], [{"k", "b"}], defopts

    assert {:error, %{error: _, key: ["k"]}} = LQRC.update :memberschema, ["x"], [{"k", "c"}], defopts
  end

  defp genkey, do: :base64.encode(:erlang.term_to_binary :erlang.now)
end
