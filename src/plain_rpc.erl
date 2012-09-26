%% ----------------------------------------------------------------------------
%%
%% plain_rpc: RPC module to accompany plain_fsm
%%
%% Copyright 2011-2012 (c) Trifork A/S.  All Rights Reserved.
%% http://trifork.com/ info@trifork.com
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

-module(plain_rpc).
-author('Kresten Krab Thorup <krab@trifork.com>').

-export([send_call/2, receive_reply/1, send_reply/2, call/2, call/3, cast/2]).

-include("include/plain_rpc.hrl").


send_call(PID, Request) ->
    Ref = erlang:monitor(process, PID),
    PID ! ?CALL({self(), Ref}, Request),
    Ref.

cast(PID, Msg) ->
    PID ! ?CAST(self(), Msg).

receive_reply(MRef) ->
    receive
        ?REPLY(MRef, Reply) ->
            erlang:demonitor(MRef, [flush]),
            Reply;
        {'DOWN', MRef, _, _, Reason} ->
            exit(Reason)
    end.

send_reply({PID,Ref}, Reply) ->
    _ = erlang:send(PID, ?REPLY(Ref, Reply)),
    ok.

call(PID,Request) ->
    call(PID, Request, infinity).

call(PID,Request,Timeout) ->
    MRef = erlang:monitor(process, PID),
    PID ! ?CALL({self(), MRef}, Request),
    receive
        ?REPLY(MRef, Reply) ->
            erlang:demonitor(MRef, [flush]),
            Reply;
        {'DOWN', MRef, _, _, Reason} ->
            exit(Reason)
    after Timeout ->
            erlang:demonitor(MRef, [flush]),
            exit({rpc_timeout, Request})
    end.


