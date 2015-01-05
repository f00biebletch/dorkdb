-module(dork).

-export([start/0, stop/0]).

start() ->
    process_flag(trap_exit, true),
    {ok, Db} = dorkdb:start_link(),
    repl(Db).

stop() ->
    dorkdb:stop().

parse(Line) ->
    [C|Rest] = string:tokens(string:strip(Line, right, $\n), " "),
    Cmd = list_to_atom(string:to_lower(C)),
    case Cmd of
	get -> 
	    [Key|[]] = Rest,
	    {get, Key};
	set ->
	    [Key|[Vals|[]]] = Rest,
	    {Val, _} = string:to_integer(Vals),
	    {set, Key, Val};
	unset ->
	    [Key|[]] = Rest,
	    {unset, Key};
	numequalto ->
	    [Vals|[]] = Rest,
	    {Val, _} = string:to_integer(Vals),
	    {numequalto, Val};
	'end' ->
	    {'end'};
	'begin' ->
	    {'begin'};
	commit ->
	    {commit};
	rollback ->
	    {rollback};
	Other ->
	    {badmatch, Other}
    end.

repl(Db) ->
    R = case io:get_line(standard_io, "dork>") of
	    eof -> {'end'};
	    {error, _Term} -> {'end'};
	    Input ->
		case catch parse(Input) of
		    {'EXIT', E} ->
			{badmatch, E};
		    Cmd ->
			gen_server:call(Db, Cmd, infinity)
		end
	end,
    io:format("~p~n",[R]),
    case R of
	{'end'} -> ok;
	_ ->
	    repl(Db)
    end.


	    
	    
