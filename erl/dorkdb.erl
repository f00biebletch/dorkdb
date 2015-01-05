-module(dorkdb).

-behaviour(gen_server).

-define(SCOPE,{local, dorkdb}).

-record(state, {journal, data, index}).

-export([start_link/0,terminate/2, stop/0]).
-export([init/1, handle_call/3, handle_cast/2,handle_info/2,
        code_change/3]).

init(_Arg) ->
    process_flag(trap_exit, true),
    logger:debug("~p:init(): starting",[?MODULE]),

    {ok, #state{journal=[], data=dict:new(), index=dict:new()}}.

start_link() ->
    gen_server:start_link(?SCOPE, ?MODULE, [], []).

terminate(_Reason, _State) ->
    logger:info("~p:terminate()",[?MODULE]),
    ok.

%% only set and unset (mutate) commands go to journal
%% for read commands, if no log, just exec, otherwise, play journal then exec
%% FIXIT genericize based on mutate/non-mutate commands along with jounral
%% FIXIT assign to Cmd and invoke exec?
handle_call({'end'}, _From, State) -> 
    {reply, {'end'}, State};
handle_call({get, K}, _From, State=#state{journal=[]}) ->
    {reply, do_get(K, State), State};
handle_call({get, K},_From, State) ->
    {reply, do_get(K, play_journal(State)), State};
handle_call({set, K, V}, _From, State=#state{journal=[]}) ->
    {reply, V, do_set(K, V, State)};
handle_call(Cmd={set, _K, V}, _From, State=#state{journal=[Cur|Rest]}) ->
    {reply, V, State#state{journal=[[Cmd|Cur]|Rest]}};
handle_call({unset, K}, _From, State=#state{journal=[]}) ->
    {reply, ok, do_unset(K, State)};
handle_call(Cmd={unset, K}, _From, State=#state{journal=[Cur|Rest]}) ->
    {reply, K, State#state{journal=[[Cmd|Cur]|Rest]}};
handle_call({numequalto, V}, _From, State=#state{journal=[]}) ->
    {reply, do_numequalto(V, State), State};
handle_call({numequalto, V}, _From, State) ->
    {reply, do_numequalto(V, play_journal(State)), State};
handle_call(transaction, _From, State=#state{journal=Cur}) ->
    {reply, ok, State#state{journal=[journal:new()|Cur]}};
handle_call(Msg, _From, State) ->
    io:format("BAH!!~n",[]),
    {reply, Msg, State}.

handle_cast(stop, State) ->
    {stop, normal, State};
handle_cast(Msg, State) ->
    {Msg, normal, State}.

handle_info(_Msg, State) ->
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
     {ok, State}.
stop() -> gen_server:cast(?MODULE, stop).

play_journal(Db=#state{journal=J}) ->
    lists:foldl(play_log, Db, J).

play_log(Log, Db) ->
    lists:foldl(fun(Cmd, Db1) -> exec(Cmd, Db1) end, Db, Log).

exec({set, K, V}, Db) -> do_set(K, V, Db);
exec({unset, K}, Db) -> do_unset(K, Db);
exec({numequalto, K}, Db) -> do_numequalto(K, Db);
exec({get, K}, Db) -> do_get(K, Db).

do_get(Key, Db) -> 
    case dict:find(Key, Db#state.data) of
	error -> nothing;
	{ok, V} -> V
    end.

do_set(Key, Val, Db) -> 
    Db#state{data=dict:store(Key, Val, Db#state.data),
	     index=dict:update_counter(Val, 1, Db#state.index)}.

do_unset(Key, Db) ->
    Val = case dict:find(Key, Db#state.data) of
	      error -> nothing;
	      {ok, V} -> V
	  end,
    Db#state{data=dict:erase(Key, Db#state.data),
	     index=dict:update_counter(Val, 0, Db#state.index)}.

do_numequalto(Key, Db) ->
    case dict:find(Key, Db#state.index) of
	error -> -1;
	{ok, V} -> V
    end.
			  
other_numequalto(To, Db) ->
    dict:fold(fun(_K,V,Acc) -> case (V == To) of true -> Acc+1; _ -> Acc end end, 0, Db#state.data).

    
