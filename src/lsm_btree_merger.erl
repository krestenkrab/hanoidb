-module(fractal_btree_merger).

%%
%% Naive Merge of two b-trees.  A better implementation should iterate leafs, not KV's
%%

-export([merge/4]).

-record(state, { out, a_pid, b_pid }).

merge(A,B,C, Size) ->
    {ok, Out} = fractal_btree_writer:open(C, Size),
    Owner = self(),
    PID1 = spawn_link(fun() -> scan(Owner, A) end),
    PID2 = spawn_link(fun() -> scan(Owner, B) end),

    %% "blocks" until both scans are done ...
    {ok, Count} = receive_both(undefined, undefined, #state{ out=Out, a_pid=PID1, b_pid=PID2 }, 0),

    %% finish stream tree
    ok = fractal_btree_writer:close(Out),

    {ok, Count}.


scan(SendTo,FileName) ->
    %% yes, we need a separate file to scan it, since pread doesn't do read-ahead
    {ok, File} = fractal_btree_reader:open(FileName),
    fractal_btree_reader:fold(fun(K,V,_) ->
                              SendTo ! {ok, self(), K, V}
                      end,
                      ok,
                      File),
    fractal_btree_reader:close(File),
    SendTo ! {eod, self()},
    ok.


receive_both(undefined, BVal, #state{a_pid=PID1}=State, Count) ->
    receive
        {ok, PID1, Key1, Value1} ->
            receive_both({Key1,Value1}, BVal, State, Count);

        {eod, PID1} ->
            case BVal of
                {Key2, Value2} ->
                    fractal_btree_writer:add(State#state.out, Key2, Value2),
                    receive_bonly(State, Count+1);

                undefined ->
                    receive_bonly(State, Count)
            end
    end;

receive_both({Key1,Value1}=AValue, undefined, #state{ b_pid=PID2 }=State, Count) ->
    receive
        {ok, PID2, Key2, Value2} ->
            receive_both(AValue, {Key2,Value2}, State, Count);

        {eod, PID2} ->
            ok = fractal_btree_writer:add(State#state.out, Key1, Value1),
            receive_aonly(State, Count+1)
    end;

receive_both(AValue={Key1,Value1}, BValue={Key2,Value2}, State, Count) ->

    if Key1 < Key2 ->
            ok = fractal_btree_writer:add(State#state.out, Key1, Value1),
            receive_both(undefined, BValue, State, Count+1);

       Key2 < Key1 ->
            ok = fractal_btree_writer:add(State#state.out, Key2, Value2),
            receive_both(AValue, undefined, State, Count+1);

       Key1 == Key2 ->
            %% TODO: eliminate tombstones, right now they just bubble down
            ok = fractal_btree_writer:add(State#state.out, Key2, Value2),
            receive_both(undefined, undefined, State, Count+1)
    end.

%%
%% Reached the end of the "B File" ... now just stream everything from A to OUT
%%
receive_aonly(#state{a_pid=PID1}=State, Count) ->
    receive
        {ok, PID1, Key1, Value1} ->
            ok = fractal_btree_writer:add(State#state.out,
                                         Key1,
                                         Value1),
            receive_aonly(State, Count+1);
        {eod, PID1} ->
            {ok, Count}
    end.

%%
%% Reached the end of the "A File" ... now just stream everything from B to OUT
%%
receive_bonly(#state{b_pid=PID2}=State, Count) ->
    receive
        {ok, PID2, Key2, Value2} ->
            ok = fractal_btree_writer:add(State#state.out,
                                         Key2,
                                         Value2),
            receive_bonly(State, Count+1);
        {eod, PID2} ->
            {ok, Count}
    end.


