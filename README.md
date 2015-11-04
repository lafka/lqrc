# LRQC

**Deprecated**: There was no time to do this properly, use :riakc directly in a simple wrapper class.

Name really has no meaning anymore, it had something todo with Local Riak Query Client
but remains unchanged for historical reasons. Currently this serves as
a test for working with Riak 2.0.

Provide a clean interface for writing domain specific objects to Riak
and possibly maintaining a PUB/SUB notification system to synchronise
events across a cluster using `redq` and `redis`.


**This is highly experimental, API will break your data will be lost**

## @todo

+ Integrate with Yokozuna (requires upstream changes)
+ Add redq hooks for publishing events
+ Add support for subtypes (requires update to bucket properties)
 + Should be able to query for collection of children, uses Yokozuna
   or m/r using set aggregates in parent object
 + 'Schema' configuration must be stored in Riak bucket-type properties

## Example

```elixir
# Create a new domain :network containing the subdomain :device
LQRC.Domain.create :network, [yz_index: "network", lqrc_schema: [sub: [:device]]]
# => :ok

LQRC.Domain.read :network
# => {:ok, [yz_index: "network", young_vclock: 20, w: quorum, small_vclock: 50
      rw: quorum, r: quorum, pw: 0, precommit: [], pr: 0, postcommit: [],
      old_vclock: 86400, notfound_ok: true, n_val: 3, last_write_wins: false,
      linkfun: {modfun,riak_kv_wm_link_walker,mapreduce_linkfun}, dw: quorum,
      chash_keyfun: [riak_core_util: chash_std_keyfun}, big_vclock: 50,
      basic_quorum: false, allow_mult: true, active: true, claimant: 'riak@universe',
      lqrc_schema: [sub: [:device]]]}

# Use a namespace
ns = "myns"

# Read terms from Riak
LQRC.write :network, [ns, "PhrQz"], [name: "Test network"]
=> :ok

# Add some devices
LQRC.write :device, [ns, "PhrQz", "e2a5b9"], [name: "Test gw #1", type: "gateway"]
# => ok
LQRC.write :device, [ns, "PhrQz", "e2a5ba"], [name: "Test dev #2", type: "router"]
# => ok

LQRC.read :network, [ns, "PhrQz"]
# => {:ok, [key: "PhrQz", name: "Test network"]}

# Read terms, expand the keys of the subdomain :device
LQRC.read :network, [ns, "PhrQz"], expand: [:device]
# => {:ok, [key: "PhrQz", name: "Test network", devices: ["e2a5b9", "e2a5ba"]]}

# Read terms, expand the full body of subdomain :device
LQRC.read :network, [ns, "PhrQz"], expand: [device: :body]
# => {:ok, [key: "PhrQz", name: "Test network", devices: [
	"e2a5b9": [key: "e2a5b9", name: "Test gw #1"],
	"e2a5ba": [key: "e2a5ba", name: "Test dev #2"]]]}

# Query on Yokuzuna indexes
LQRC.query :device, [ns, "PhrQz"], [type: "gateway"]
# => {:ok, [key: "e2a5b9", name: "Test gw #1", type: "gateway"]}
```


## Making search work

Yokozuna needs some love:

- Create indexes (or cores whatever):
  `:riakc_pb_socket.create_search_index p, "network", "network", []`
