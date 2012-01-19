-module(lsm_btree_fold_worker).

%%
%% This worker is used to merge fold results from individual
%% levels. First, it receives a message
%%
%%  {initialize, [LevelWorker, ...]}
%%
%%  And then from each LevelWorker, a sequence of
%%
%%   {level_result, LevelWorker, Key1, Value}
%%   {level_result, LevelWorker, Key2, Value}
%%   {level_result, LevelWorker, Key3, Value}
%%   {level_result, LevelWorker, Key4, Value}
%%   ...
%%   {level_done, LevelWorker}
%%
%% The order of level workers in the initialize messge is top-down,
%% which is used to select between same-key messages from different
%% levels.
%%
%% This fold_worker process will then send to a designated SendTo target
%% a similar sequence of messages
%%
%%   {fold_result, self(), Key1, Value}
%%   {fold_result, self(), Key2, Value}
%%   {fold_result, self(), Key3, Value}
%%   ...
%%   {fold_done, self()}.
%%

-export([start/1]).
-behavior(plain_fsm).
-export([data_vsn/0, code_change/3]).

-include("lsm_btree.hrl").

-record(state, {sendto}).

start(SendTo) ->
    PID = plain_fsm:spawn(?MODULE,
                          fun() ->
                                  process_flag(trap_exit,true),
                                  initialize(#state{sendto=SendTo}, [])
                          end),
    {ok, PID}.


initialize(State, PrefixFolders) ->

    Parent = plain_fsm:info(parent),
    receive
        {prefix, [_]=Folders} ->
            initialize(State, Folders);

        {initialize, Folders} ->

            Initial = [ {PID,undefined} || PID <- (PrefixFolders ++ Folders) ],
            fill(State, Initial, PrefixFolders ++ Folders);

        %% gen_fsm handling
        {system, From, Req} ->
            plain_fsm:handle_system_msg(
              From, Req, State, fun(S1) -> initialize(S1, PrefixFolders) end);

        {'EXIT', Parent, Reason} ->
            plain_fsm:parent_EXIT(Reason, State)
    end.


fill(State, Values, []) ->
    emit_next(State, Values);

fill(State, Values, [PID|Rest]=PIDs) ->
    receive
        {level_done, PID} ->
            fill(State, lists:keydelete(PID, 1, Values), Rest);
        {level_result, PID, Key, Value} ->
            fill(State, lists:keyreplace(PID, 1, Values, {PID,{Key,Value}}), Rest);

        %% gen_fsm handling
        {system, From, Req} ->
            plain_fsm:handle_system_msg(
              From, Req, State, fun(S1) -> fill(S1, Values, PIDs) end);

        {'EXIT', Parent, Reason}=Msg ->
            case plain_fsm:info(parent) == Parent of
                true ->
                    plain_fsm:parent_EXIT(Reason, State);
                false ->
                    error_logger:info_msg("unhandled EXIT message ~p~n", [Msg]),
                    fill(State, Values, PIDs)
            end

    end.

emit_next(State, []) ->
    State#state.sendto ! {fold_done, self()},
    ok;

emit_next(State, [{FirstPID,FirstKV}|Rest]=Values) ->

    {{FoundKey, FoundValue}, FillFrom} =
        lists:foldl(fun({P,{K1,_}=KV}, {{K2,_},_}) when K1 < K2 ->
                            {KV,[P]};
                       ({P,{K,_}}, {{K,_}=KV,List}) ->
                            {KV, [P|List]};
                       (_, Found) ->
                            Found
                    end,
                    {FirstKV,[FirstPID]},
                    Rest),

    case FoundValue of
        ?TOMBSTONE ->
            ok;
        _ ->
            State#state.sendto ! {fold_result, self(), FoundKey, FoundValue}
    end,

    fill(State, Values, FillFrom).


data_vsn() ->
    5.

code_change(_OldVsn, _State, _Extra) ->
    {ok, {#state{}, data_vsn()}}.

