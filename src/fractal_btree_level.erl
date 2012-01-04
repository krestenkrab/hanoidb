-module(fractal_btree_level).

%%
%% Manages a "pair" of fractal_index (or rathern, 0, 1 or 2), and governs
%% the process of injecting/merging parent trees into this pair.
%%

-behavior(plain_fsm).
-export([data_vsn/0, code_change/3]).

-export([open/2, lookup/2, inject/2, close/1]).

-include_lib("kernel/include/file.hrl").

-record(state, {
          a, b, next, dir, level, inject_done_ref
          }).



open(Dir,Level) ->
    plain_fsm:spawn_link(?MODULE,
                         fun() ->
                            process_flag(trap_exit,true),
                            initialize(#state{dir=Dir,level=Level})
                         end).

lookup(Ref, Key) ->
    call(Ref, {lookup, Key}).

inject(Ref, FileName) ->
    call(Ref, {inject, FileName}).

close(Ref) ->
    call(Ref, close).


data_vsn() ->
    5.

code_change(_OldVsn, _State, _Extra) ->
    {ok, {#state{}, data_vsn()}}.


-define(REQ(From,Msg), {'$req', From, Msg}).
-define(REPLY(Ref,Msg), {'$rep', Ref, Msg}).

send_request(PID, Request) ->
    Ref = erlang:monitor(PID),
    PID ! ?REQ({self(), Ref}, Request),
    Ref.

receive_reply(MRef) ->
    receive
        ?REPLY(MRef, Reply) ->
            erlang:demonitor(MRef, [flush]),
            Reply;
		{'DOWN', MRef, _, _, Reason} ->
		    exit(Reason)
    end.

call(PID,Request) ->
    Ref = send_request(PID, Request),
    receive_reply(Ref).

reply({PID,Ref}, Reply) ->
    erlang:send(PID, ?REPLY(Ref, Reply)),
    ok.


initialize(State) ->
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
                    main_loop(State#state{ a=BT1, b=BT2 });

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


main_loop(State = #state{ a=undefined, b=undefined }) ->
    Parent = plain_fsm:info(parent),
    receive
        ?REQ(From, {lookup, _})=Msg ->
            case State#state.next of
                undefined ->
                    reply(From, notfound);
                Next ->
                    Next ! Msg
            end,
            main_loop(State);

        ?REQ(From, {inject, FileName}) ->
            {ok, BT} = fractal_btree_reader:open(FileName),
            reply(From, ok),
            main_loop(State#state{ a=BT });

        ?REQ(From, close) ->
            reply(From, ok),
            ok;

        %% gen_fsm handling
        {system, From, Req} ->
            plain_fsm:handle_system_msg(
              From, Req, State, fun(S1) -> main_loop(S1) end);
        {'EXIT', Parent, Reason} ->
            plain_fsm:parent_EXIT(Reason, State)
    end;

main_loop(State = #state{ a=BT1, b=undefined, next=Next }) ->
    Parent = plain_fsm:info(parent),
    receive
        ?REQ(From, {lookup, Key})=Req ->
            case fractal_btree_reader:lookup(BT1, Key) of
                {ok, deleted} ->
                    reply(From, notfound);
                {ok, _}=Reply ->
                    reply(From, Reply);
                notfound when Next =:= undefined ->
                    reply(From, notfound);
                notfound ->
                    Next ! Req
            end,
            main_loop(State);

        ?REQ(From, {inject, FileName}) ->
            {ok, BT2} = fractal_btree_reader:open(FileName),
            reply(From, ok),
            ok = begin_merge(State),
            main_loop(State#state{ b=BT2 });

        ?REQ(From, close) ->
            fractal_btree_reader:close(BT1),
            reply(From, ok),
            ok;

        %% gen_fsm handling
        {system, From, Req} ->
            plain_fsm:handle_system_msg(
              From, Req, State, fun(S1) -> main_loop(S1) end);
        {'EXIT', Parent, Reason} ->
            plain_fsm:parent_EXIT(Reason, State)
    end;

main_loop(State = #state{ next=Next }) ->
    Parent = plain_fsm:info(parent),
    receive
        ?REQ(From, {lookup, Key})=Req ->
            case fractal_btree_reader:lookup(State#state.b, Key) of
                {ok, deleted} ->
                    reply(From, notfound),
                    main_loop(State);
                {ok, _}=Reply ->
                    reply(From, Reply),
                    main_loop(State);
                _ ->
                    case fractal_btree_reader:lookup(State#state.a, Key) of
                        {ok, deleted} ->
                            reply(From, notfound);
                        {ok, _}=Reply ->
                            reply(From, Reply);
                        notfound when Next =:= undefined ->
                            reply(From, notfound);
                        notfound ->
                            Next ! Req
                    end,
                    main_loop(State)
            end;


        ?REQ(From, close) ->
            fractal_btree_reader:close(State#state.a),
            fractal_btree_reader:close(State#state.b),
            %% TODO: stop merger, if any?
            reply(From, ok),
            ok;

        %%
        %% The outcome of merging resulted in a file with less than
        %% level #entries, so we keep it at this level
        %%
        {merge_done, Count, OutFileName} when Count =< State#state.level ->

            % first, rename the tmp file to C, so recovery will pick it up
            CFileName = filename("C",State),
            ok = file:rename(OutFileName, CFileName),

            % then delete A and B (if we crash now, C will become the A file)
            {ok, State2} = close_a_and_b(State),

            % then, rename C to A, and open it
            AFileName = filename("A",State2),
            ok = file:rename(CFileName, AFileName),
            {ok, BT} = fractal_btree_reader:open(AFileName),

            main_loop(State2#state{ a=BT, b=undefined });

        %%
        %% We need to push the output of merging to the next level
        %%
        {merge_done, _, OutFileName} ->
            State1 =
                if Next =:= undefined ->
                        PID = open(State#state.dir, State#state.level * 2),
                        State#state{ next=PID };
                   true ->
                        State
                end,

            MRef = send_request(State1#state.next, {inject, OutFileName}),
            main_loop(State1#state{ inject_done_ref = MRef });

        %%
        %% Our successor accepted the inject
        %%
        ?REPLY(MRef, ok) when MRef =:= State#state.inject_done_ref ->
            erlang:demonitor(MRef, [flush]),
            {ok, State2} = close_a_and_b(State),
            main_loop(State2#state{ inject_done_ref=undefined, a=undefined, b=undefined });

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



begin_merge(State) ->
    AFileName = filename("A",State),
    BFileName = filename("B",State),
    XFileName = filename("X",State),
    Owner = self(),

    spawn_link(fun() ->
                       {ok, OutCount} = fractal_btree_merger:merge(AFileName, BFileName, XFileName,
                                                                   State#state.level * 2),
                       Owner ! {merge_done, OutCount, XFileName}
               end),

    ok.


close_a_and_b(State) ->
    AFileName = filename("A",State),
    BFileName = filename("B",State),

    ok = fractal_btree_reader:close(State#state.a),
    ok = fractal_btree_reader:close(State#state.b),

    ok = file:delete(AFileName),
    ok = file:delete(BFileName),

    {ok, State#state{a=undefined, b=undefined}}.


filename(PFX, State) ->
    filename:join(State#state.dir, PFX ++ integer_to_list(State#state.level) ++ ".data").

