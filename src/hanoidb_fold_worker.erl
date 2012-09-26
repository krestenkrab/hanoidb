%% ----------------------------------------------------------------------------
%%
%% hanoidb: LSM-trees (Log-Structured Merge Trees) Indexed Storage
%%
%% Copyright 2011-2012 (c) Trifork A/S.  All Rights Reserved.
%% http://trifork.com/ info@trifork.com
%%
%% Copyright 2012 (c) Basho Technologies, Inc.  All Rights Reserved.
%% http://basho.com/ info@basho.com
%%
%% This file is provided to you under the Apache License, Version 2.0 (the
%% "License"); you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
%% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
%% License for the specific language governing permissions and limitations
%% under the License.
%%
%% ----------------------------------------------------------------------------

-module(hanoidb_fold_worker).
-author('Kresten Krab Thorup <krab@trifork.com>').

-ifdef(DEBUG).
-define(log(Fmt,Args),io:format(user,Fmt,Args)).
-else.
-define(log(Fmt,Args),ok).
-endif.

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
%%   {level_results, LevelWorker, [{Key,Value}...]} %% alternatively
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

-include("hanoidb.hrl").
-include("plain_rpc.hrl").

-record(state, {sendto :: pid(), sendto_ref :: reference()}).

start(SendTo) ->
    F = fun() ->
                ?log("fold_worker started ~p~n", [self()]),
                process_flag(trap_exit, true),
                MRef = erlang:monitor(process, SendTo),
                try
                    initialize(#state{sendto=SendTo, sendto_ref=MRef}, []),
                    ?log("fold_worker done ~p~n", [self()])
                catch
                    Class:Ex ->
                        ?log("fold_worker exception  ~p:~p ~p~n", [Class, Ex, erlang:get_stacktrace()]),
                        error_logger:error_msg("Unexpected: ~p:~p ~p~n", [Class, Ex, erlang:get_stacktrace()]),
                        exit({bad, Class, Ex, erlang:get_stacktrace()})
                end
        end,
    PID = plain_fsm:spawn(?MODULE, F),
    {ok, PID}.

initialize(State, PrefixFolders) ->
    Parent = plain_fsm:info(parent),
    receive
        {prefix, [_]=Folders} ->
            initialize(State, Folders);

        {initialize, Folders} ->
            Queues  = [ {PID,queue:new()} || PID <- (PrefixFolders ++ Folders) ],
            Initial = [ {PID,undefined} || PID <- (PrefixFolders ++ Folders) ],
            fill(State, Initial, Queues, PrefixFolders ++ Folders);

        %% gen_fsm handling
        {system, From, Req} ->
            plain_fsm:handle_system_msg(
              From, Req, State, fun(S1) -> initialize(S1, PrefixFolders) end);

        {'DOWN', MRef, _, _, _} when MRef =:= State#state.sendto_ref ->
            ok;

        {'EXIT', Parent, Reason} ->
            plain_fsm:parent_EXIT(Reason, State)
    end.

fill(State, Values, Queues, []) ->
    emit_next(State, Values, Queues);

fill(State, Values, Queues, [PID|Rest]=PIDs) ->
%    io:format(user, "v=~P, q=~P, pids=~p~n", [Values, 10, Queues, 10, PIDs]),
    case lists:keyfind(PID, 1, Queues) of
        {PID, Q} ->
            case queue:out(Q) of
                {empty, Q} ->
                    fill_from_inbox(State, Values, Queues, [PID], PIDs);

                {{value, Msg}, Q2} ->
                    Queues2 = lists:keyreplace(PID, 1, Queues, {PID, Q2}),

                    case Msg of
                        done ->
                            fill(State, lists:keydelete(PID, 1, Values), Queues2, Rest);
                        {_Key, _Value}=KV ->
                            fill(State, lists:keyreplace(PID, 1, Values, {PID, KV}), Queues2, Rest)
                    end
            end
    end.

fill_from_inbox(State, Values, Queues, [], PIDs) ->
    fill(State, Values, Queues, PIDs);

fill_from_inbox(State, Values, Queues, [PID|_]=PIDs, SavePIDs) ->
    ?log("waiting for ~p~n", [PIDs]),
    receive
        {level_done, PID} ->
            ?log("got {done, ~p}~n", [PID]),
            Queues2 = enter(PID, done, Queues),
            fill_from_inbox(State, Values, Queues2, lists:delete(PID,PIDs), SavePIDs);

        {level_limit, PID, Key} ->
            ?log("got {limit, ~p}~n", [PID]),
            Queues2 = enter(PID, {Key, limit}, Queues),
            fill_from_inbox(State, Values, Queues2, lists:delete(PID,PIDs), SavePIDs);

        {level_result, PID, Key, Value} ->
            ?log("got {result, ~p}~n", [PID]),
            Queues2 = enter(PID, {Key, Value}, Queues),
            fill_from_inbox(State, Values, Queues2, lists:delete(PID,PIDs), SavePIDs);

        ?CALL(From,{level_results, PID, KVs}) ->
            ?log("got {results, ~p}~n", [PID]),
            plain_rpc:send_reply(From,ok),
            Queues2 = enter_many(PID, KVs, Queues),
            fill_from_inbox(State, Values, Queues2, lists:delete(PID,PIDs), SavePIDs);

        %% gen_fsm handling
        {system, From, Req} ->
            plain_fsm:handle_system_msg(
              From, Req, State, fun(S1) -> fill_from_inbox(S1, Values, Queues, PIDs, SavePIDs) end);

        {'DOWN', MRef, _, _, _} when MRef =:= State#state.sendto_ref ->
            ok;

        {'EXIT', Parent, Reason}=Msg ->
            case plain_fsm:info(parent) == Parent of
                true ->
                    plain_fsm:parent_EXIT(Reason, State);
                false ->
                    error_logger:info_msg("unhandled EXIT message ~p~n", [Msg]),
                    fill_from_inbox(State, Values, Queues, PIDs, SavePIDs)
            end

    end.

enter(PID, Msg, Queues) ->
    {PID, Q} = lists:keyfind(PID, 1, Queues),
    Q2 = queue:in(Msg, Q),
    lists:keyreplace(PID, 1, Queues, {PID, Q2}).

enter_many(PID, Msgs, Queues) ->
    {PID, Q} = lists:keyfind(PID, 1, Queues),
    Q2 = lists:foldl(fun queue:in/2, Q, Msgs),
    lists:keyreplace(PID, 1, Queues, {PID, Q2}).

emit_next(State, [], _Queues) ->
    ?log( "emit_next ~p~n", [[]]),
    Msg =  {fold_done, self()},
    Target = State#state.sendto,
    ?log( "~p ! ~p~n", [Target, Msg]),
    _ = plain_rpc:cast(Target, Msg),
    end_of_fold(State);

emit_next(State, [{FirstPID,FirstKV}|Rest]=Values, Queues) ->
    ?log( "emit_next ~p~n", [Values]),
    case
        lists:foldl(fun({P,{K1,_}=KV}, {{K2,_},_}) when K1 < K2 ->
                            {KV,[P]};
                       ({P,{K,_}}, {{K,_}=KV,List}) ->
                            {KV, [P|List]};
                       (_, Found) ->
                            Found
                    end,
                    {FirstKV,[FirstPID]},
                    Rest)
    of
        {{_, ?TOMBSTONE}, FillFrom} ->
            fill(State, Values, Queues, FillFrom);
        {{Key, limit}, _} ->
            ?log( "~p ! ~p~n", [State#state.sendto, {fold_limit, self(), Key}]),
            _ = plain_rpc:cast(State#state.sendto, {fold_limit, self(), Key}),
            end_of_fold(State);
        {{Key, Value}, FillFrom} ->
            ?log( "~p ! ~p~n", [State#state.sendto, {fold_result, self(), Key, '...'}]),
            plain_rpc:call(State#state.sendto, {fold_result, self(), Key, Value}),
            fill(State, Values, Queues, FillFrom)
    end.

end_of_fold(_State) ->
    ok.

data_vsn() ->
    5.

code_change(_OldVsn, _State, _Extra) ->
    {ok, {#state{}, data_vsn()}}.


