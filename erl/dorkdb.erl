-module(dorkdb).

-behaviour(gen_server).

-define(SCOPE,{local, dorkdb}).

-record(db, {journal, data, index}).

-export([start_link/0,terminate/2, stop/0]).
-export([init/1, handle_call/3, handle_cast/2,handle_info/2,
        code_change/3]).

init(_Arg) ->
    process_flag(trap_exit, true),
    logger:debug("~p:init(): starting",[?MODULE]),

    {ok, #db{journal=[], data=dict:new(), index=dict:new()}}.

start_link() ->
    gen_server:start_link(?SCOPE, ?MODULE, [], []).

terminate(_Reason, _State) ->
    logger:info("~p:terminate()",[?MODULE]),
    ok.

%% only set and unset (mutate) commands go to journal
%% for read commands, if no log, just exec, otherwise, play journal then exec
%% FIXIT genericize based on mutate/non-mutate commands along with jounral
%% FIXIT assign to Cmd and invoke exec?
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
    
handle_call(C={set, _K, _V}, _From, Db=#db{journal=[]}) ->
    raw_write(C, Db);
handle_call(C={set, _K, _V}, _From, Db) ->
    journal_write(C, Db);
handle_call(C={unset, _K}, _From, Db=#db{journal=[]}) ->
    raw_write(C, Db);
handle_call(C={unset, _K}, _From, Db) ->
    journal_write(C, Db);

handle_call({transaction}, _From, Db=#db{journal=Cur}) ->
    {reply, ok, Db#db{journal=[journal:new()|Cur]}};
handle_call(Msg, _From, Db) ->
    io:format("BAH!!~n",[]),
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
    lists:foldl(play_log, Db, J).

play_log(Log, Db) ->
    lists:foldl(fun(Cmd, Db1) -> exec(Cmd, Db1) end, Db, Log).

exec({set, K, V}, Db) ->
    Db#db{data=dict:store(K, V, Db#db.data),
	     index=dict:update_counter(V, 1, Db#db.index)};
exec({unset, K}, Db) ->
    Val = case dict:find(K, Db#db.data) of
	      error -> nothing;
	      {ok, V} -> V
	  end,
    Db#db{data=dict:erase(K, Db#db.data),
	     index=dict:update_counter(Val, 0, Db#db.index)};
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
			  
other_numequalto(To, Db) ->
    dict:fold(fun(_K,V,Acc) -> case (V == To) of true -> Acc+1; _ -> Acc end end, 0, Db#db.data).

    
