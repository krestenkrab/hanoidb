-module(qtop).

-export([max/0, max/1, queue/2, queue/1]).

max() ->
    max(5).

max(N) ->
    PIDs = erlang:processes(),
    Pairs = lists:foldl(fun(PID,Acc) ->
                                case erlang:process_info(PID, message_queue_len) of
                                    {message_queue_len, Len} ->
                                        [{Len, PID}|Acc];
                                    _ ->
                                        Acc
                                end
                        end,
                        [],
                        PIDs),
    [{_, MaxPID}|_] = lists:reverse(lists:sort(Pairs)),
    queue(MaxPID,N).

queue(PID) ->
    queue(PID, 5).

queue(PID, N) when is_list(PID) ->
    queue(erlang:list_to_pid(PID), N);
queue(MaxPID, N) ->
    {message_queue_len, MaxLen} = erlang:process_info(MaxPID, message_queue_len),
    {messages, Msgs} = erlang:process_info(MaxPID, messages),
    {Front30,_} = lists:split(min(N,length(Msgs)), Msgs),
    io:format("==== PID: ~p, qlen:~p~n", [MaxPID,MaxLen]),
    lists:foldl( fun(Msg,M) ->
                         io:format("[~p]: ~P~n", [M, Msg,30]),
                         M+1
                 end,
                 1,
                 Front30),
    ok.

