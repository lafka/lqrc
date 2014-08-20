# curl -XPUT http://localhost:8098/search/index/user
# bin/riak-admin bucket-type create user '{"props":{"search_index":"user"}}'     
# bin/riak-admin bucket-type activate user

ExUnit.start

Code.require_file "utils.exs", __DIR__

