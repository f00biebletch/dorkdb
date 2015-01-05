-module(dork).

-export([start/0, stop/0]).

start() ->
    {ok, Db} = dorkdb:start_link(),
    repl(Db).

stop() ->
    dorkdb:stop().

parse(Line) ->
    %io:format("got ~p~n", [Line]),
    [C|Rest] = string:tokens(string:strip(Line, right, $\n), " "),
    Cmd = list_to_atom(string:to_lower(C)),
    io:format("cmd = ~p", [Cmd]),
    case Rest of
	[K] ->
	    {Cmd, string:strip(K, right, $\n)};
	[K, V] -> 
	    {Arg, _} = string:to_integer(V),
	    {Cmd, K, Arg};
	[] -> {Cmd}
    end.

repl(Db) ->
    R = case io:get_line(standard_io, "dork>") of
	    eof -> {done};
	    {error, _Term} -> {done};
	    Input -> 
		gen_server:call(Db, parse(Input), infinity)
	end,
    io:format("RESPONSE ~p~n",[R]),
    case R of
	{done} -> ok;
	_ ->
	    repl(Db)
    end.


	    
	    
