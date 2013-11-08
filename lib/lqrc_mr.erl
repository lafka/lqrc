-module(lqrc_mr).

-export([robject/3]).

% Return all the values from an object
robject(Value,_Keydata,Arg) ->
	riak_object:Arg(Value).
