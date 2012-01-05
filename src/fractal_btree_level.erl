-module(fractal_btree_level).

%%
%% Manages a "pair" of fractal_index (or rathern, 0, 1 or 2), and governs
%% the process of injecting/merging parent trees into this pair.
%%

%%
%% For now, we use plain_fsm, because we *want* selective receive to postpone
%% pending injects while we're merging.  That's a lot simpler than maintaining
%% to corresponding merge queue.
%%

-behavior(plain_fsm).
-export([data_vsn/0, code_change/3]).

-export([open/3, lookup/2, inject/2, close/1]).

-include_lib("kernel/include/file.hrl").

-record(state, {
          a, b, next, dir, level, inject_done_ref, merge_pid
          }).

%%%%% PUBLIC OPERATIONS

open(Dir,Level,Next) when Level>0 ->
    {ok, plain_fsm:spawn_link(?MODULE,
                              fun() ->
                                      process_flag(trap_exit,true),
                                      initialize(#state{dir=Dir,level=Level,next=Next})
                              end)}.

lookup(Ref, Key) ->
    call(Ref, {lookup, Key}).

inject(Ref, FileName) ->
    Result = call(Ref, {inject, FileName}),
    Result.

close(Ref) ->
    call(Ref, close).


%%%%% INTERNAL

data_vsn() ->
    5.

code_change(_OldVsn, _State, _Extra) ->
    {ok, {#state{}, data_vsn()}}.


-define(REQ(From,Msg), {'$req', From, Msg}).
-define(REPLY(Ref,Msg), {'$rep', Ref, Msg}).

send_request(PID, Request) ->
    Ref = erlang:monitor(process, PID),
    PID ! ?REQ({self(), Ref}, Request),
    Ref.

receive_reply(MRef) ->
    receive
        ?REPLY(MRef, Reply) ->
            erlang:demonitor(MRef, [flush]),
            Reply;
		{'DOWN', MRef, _, _, Reason} ->
            error_logger:info_msg("Level dies, reason=~p~n", [Reason]),
		    exit(Reason)
    end.

call(PID,Request) ->
    Ref = send_request(PID, Request),
    receive_reply(Ref).

reply({PID,Ref}, Reply) ->
    erlang:send(PID, ?REPLY(Ref, Reply)),
    ok.


initialize(State) ->
%    error_logger:info_msg("in ~p level=~p~n", [self(), State]),

    AFileName = filename("A",State),
    BFileName = filename("B",State),
    CFileName = filename("C",State),

    case file:read_file_info(CFileName) of
        {ok, _} ->

            %% recover from post-merge crash
            file:delete(AFileName),
            file:delete(BFileName),
            ok = file:rename(CFileName, AFileName),

            {ok, BT} = fractal_btree_reader:open(CFileName),
            main_loop(State#state{ a= BT, b=undefined });

        {error, enoent} ->
            case file:read_file_info(BFileName) of
                {ok, _} ->
                    {ok, BT1} = fractal_btree_reader:open(AFileName),
                    {ok, BT2} = fractal_btree_reader:open(BFileName),

                    check_begin_merge_then_loop(State#state{ a=BT1, b=BT2 });

                {error, enoent} ->

                    case file:read_file_info(AFileName) of
                        {ok, _} ->
                            {ok, BT1} = fractal_btree_reader:open(AFileName),
                            main_loop(State#state{ a=BT1 });

                        {error, enoent} ->
                            main_loop(State)
                    end
            end
    end.

check_begin_merge_then_loop(State=#state{a=BT1, b=BT2, merge_pid=undefined})
  when BT1/=undefined, BT2 /= undefined ->
    {ok, MergePID} = begin_merge(State),
    main_loop(State#state{merge_pid=MergePID });
check_begin_merge_then_loop(State) ->
    main_loop(State).

main_loop(State = #state{ next=Next }) ->
    Parent = plain_fsm:info(parent),
    receive
        ?REQ(From, {lookup, Key})=Req ->
	    case do_lookup(Key, [State#state.b, State#state.a, Next]) of
		notfound ->
		    reply(From, notfound);
		{found, Result} ->
		    reply(From, {ok, Result});
		{delegate, DelegatePid} ->
		    DelegatePid ! Req
	    end,
	    main_loop(State);

        ?REQ(From, {inject, FileName}) when State#state.b == undefined ->
	    if State#state.a == undefined ->
		    ToFileName = filename("A",State),
		    SetPos = #state.a;
	       true ->    
		    ToFileName = filename("B",State),
		    SetPos = #state.b
	    end,
	    ok = file:rename(FileName, ToFileName),
	    {ok, BT} = fractal_btree_reader:open(ToFileName),
	    reply(From, ok),
	    check_begin_merge_then_loop(setelement(SetPos, State, BT));

        ?REQ(From, close) ->
            close_if_defined(State#state.a),
            close_if_defined(State#state.b),
            stop_if_defined(State#state.merge_pid),
            reply(From, ok),
            ok;

        %%
        %% The outcome of merging resulted in a file with less than
        %% level #entries, so we keep it at this level
        %%
        {merge_done, Count, OutFileName} when Count =< (1 bsl State#state.level) ->

            % first, rename the tmp file to C, so recovery will pick it up
            CFileName = filename("C",State),
            ok = file:rename(OutFileName, CFileName),

            % then delete A and B (if we crash now, C will become the A file)
            {ok, State2} = close_a_and_b(State),

            % then, rename C to A, and open it
            AFileName = filename("A",State2),
            ok = file:rename(CFileName, AFileName),
            {ok, BT} = fractal_btree_reader:open(AFileName),

            main_loop(State2#state{ a=BT, b=undefined, merge_pid=undefined });

        %%
        %% We need to push the output of merging to the next level
        %%
        {merge_done, _, OutFileName} ->
            State1 =
                if Next =:= undefined ->
                        {ok, PID} = ?MODULE:open(State#state.dir, State#state.level + 1, undefined),
                        State#state{ next=PID };
                   true ->
                        State
                end,

            MRef = send_request(State1#state.next, {inject, OutFileName}),
            main_loop(State1#state{ inject_done_ref = MRef, merge_pid=undefined });

        %%
        %% Our successor accepted the inject
        %%
        ?REPLY(MRef, ok) when MRef =:= State#state.inject_done_ref ->
            erlang:demonitor(MRef, [flush]),
            {ok, State2} = close_a_and_b(State),
            main_loop(State2#state{ inject_done_ref=undefined });

        %%
        %% Our successor died!
        %%
		{'DOWN', MRef, _, _, Reason} when MRef =:= State#state.inject_done_ref ->
		    exit(Reason);

        %% gen_fsm handling
        {system, From, Req} ->
            plain_fsm:handle_system_msg(
              From, Req, State, fun(S1) -> main_loop(S1) end);
        {'EXIT', Parent, Reason} ->
            plain_fsm:parent_EXIT(Reason, State)

    end.

do_lookup(_Key, []) ->
    notfound;
do_lookup(_Key, [Pid]) when is_pid(Pid) ->
    {delegate, Pid};
do_lookup(Key, [undefined|Rest]) ->
    do_lookup(Key, Rest);    
do_lookup(Key, [BT|Rest]) ->
    case fractal_btree_reader:lookup(BT, Key) of
	{ok, deleted} -> notfound;
	{ok, Result}  -> {found, Result};
	notfound      -> do_lookup(Key, Rest)
    end.

close_if_defined(undefined) -> ok;
close_if_defined(BT)        -> fractal_btree_reader:close(BT).

stop_if_defined(undefined) -> ok;
stop_if_defined(MergePid) when is_pid(MergePid) ->
    erlang:exit(MergePid, shutdown).

begin_merge(State) ->
    AFileName = filename("A",State),
    BFileName = filename("B",State),
    XFileName = filename("X",State),
    Owner = self(),

    file:delete(XFileName),

    MergePID = spawn_link(fun() ->
                       {ok, OutCount} = fractal_btree_merger2:merge(AFileName, BFileName, XFileName,
                                                                   1 bsl (State#state.level + 1)),

%                       error_logger:info_msg("merge done ~p,~p -> ~p~n", [AFileName, BFileName, XFileName]),

                       Owner ! {merge_done, OutCount, XFileName}
               end),

    {ok, MergePID}.


close_a_and_b(State) ->
    AFileName = filename("A",State),
    BFileName = filename("B",State),

    ok = fractal_btree_reader:close(State#state.a),
    ok = fractal_btree_reader:close(State#state.b),

    ok = file:delete(AFileName),
    ok = file:delete(BFileName),

    {ok, State#state{a=undefined, b=undefined}}.


filename(PFX, State) ->
    filename:join(State#state.dir, PFX ++ "-" ++ integer_to_list(State#state.level) ++ ".data").

