-module(dorkdb).

-behaviour(gen_server).

-define(SCOPE,{local, dorkdb}).

-record(db, {journal, data, index}).

-export([start_link/0,terminate/2, stop/0]).
-export([init/1, handle_call/3, handle_cast/2,handle_info/2,
        code_change/3]).

init(_Arg) ->
    process_flag(trap_exit, true),
    {ok, #db{journal=[], data=dict:new(), index=dict:new()}}.

start_link() ->
    gen_server:start_link(?SCOPE, ?MODULE, [], []).

terminate(_Reason, _State) ->
    ok.

%% only set and unset (mutate) commands go to journal
%% for read commands, if no log, just exec, otherwise, play journal then exec

% read funcs
handle_call({'end'}, _From, Db) -> 
    {reply, {'end'}, Db};
handle_call(C={get, _}, _From, Db=#db{journal=[]}) ->
    raw_read(C, Db);
handle_call(C={get, _K},_From, Db) ->
    journal_read(C, Db);
handle_call(C={numequalto, _V}, _From, Db=#db{journal=[]}) ->
    raw_read(C, Db);
handle_call(C={numequalto, _V}, _From, Db) ->
    journal_read(C, Db);

% modify funcs    
handle_call(C={set, _K, _V}, _From, Db=#db{journal=[]}) ->
    raw_write(C, Db);
handle_call(C={set, _K, _V}, _From, Db) ->
    journal_write(C, Db);
handle_call(C={unset, _K}, _From, Db=#db{journal=[]}) ->
    raw_write(C, Db);
handle_call(C={unset, _K}, _From, Db) ->
    journal_write(C, Db);

% transaction stuff
handle_call({'begin'}, _From, Db=#db{journal=J}) ->
    {reply, ok, Db#db{journal=[[]|J]}};
handle_call({rollback}, _From, Db=#db{journal=[_H|T]}) ->
    {reply, ok, Db#db{journal=T}};
handle_call({rollback}, _From, Db=#db{journal=[]}) ->
    {reply, no_transaction, Db};
handle_call({commit}, _From, Db=#db{journal=[]}) ->
    {reply, no_transaction, Db};
handle_call({commit}, _From, Db) ->
    % oh commit all transactions
    % this tripped me up, I wrote it to commit per block
    Db1 = play_journal(Db),
    {reply, ok, Db1#db{journal=[]}};

handle_call({dump}, _From, Db) ->
    {reply, Db, Db};
handle_call(Msg, _From, Db) ->
    {reply, Msg, Db}.

handle_cast(stop, State) ->
    {stop, normal, State};
handle_cast(Msg, State) ->
    {Msg, normal, State}.

handle_info(_Msg, State) ->
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
     {ok, State}.
stop() -> gen_server:cast(?MODULE, stop).

raw_read(C, Db) -> {reply, exec(C, Db), Db}.
journal_read(C, Db) -> {reply, exec(C, play_journal(Db)), Db}.

raw_write(C, Db) -> {reply, ok, exec(C, Db)}.
journal_write(C, Db=#db{journal=[Cur|Rest]}) -> 
    {reply, ok, Db#db{journal=[[C|Cur]|Rest]}}.

play_journal(Db=#db{journal=J}) ->
    lists:foldr(fun play_log/2, Db, J).
play_log(Log, Db) ->
    lists:foldr(fun(Cmd, Db1) -> exec(Cmd, Db1) end, Db, Log).

exec({set, K, V}, Db) ->
    % consistency pain
    Old = exec({get, K}, Db),
    case Old of
	nothing ->
	    Db#db{data=dict:store(K, V, Db#db.data),
		  index=dict:update_counter(V, 1, Db#db.index)};
	V ->
	    Db;
	_ ->
	    Idx1=dict:update_counter(Old, -1, Db#db.index),
	    Idx2=dict:update_counter(V, 1, Idx1),
	    Db#db{data=dict:store(K, V, Db#db.data),
		  index=Idx2}
    end;
exec({unset, K}, Db) ->
    Val = exec({get, K}, Db),
    Db#db{data=dict:erase(K, Db#db.data),
	  index=dict:update_counter(Val, -1, Db#db.index)};
exec({numequalto, K}, Db) ->
    case dict:find(K, Db#db.index) of
	error -> 0;
	{ok, V} -> V
    end;
exec({get, K}, Db) ->
    case dict:find(K, Db#db.data) of
	error -> nothing;
	{ok, V} -> V
    end.
    
