-module(hanoidb_han2_backend).

-include("hanoidb.hrl").

-behavior(hanoidb_backend).

-export([open_random_reader/2, file_name/1, range_fold/4, lookup/2, close_random_reader/1]).
-export([open_batch_reader/2, read_next/1, close_batch_reader/1]).
-export([open_batch_writer/2, write_next/2, write_count/1, close_batch_writer/1]).


open_random_reader(Name, Options) ->
    hanoidb_reader:open(Name, [random|Options]).

file_name(Reader) ->
    hanoidb_reader:file_name(Reader).

lookup(Key, Reader) ->
    hanoidb_reader:lookup(Reader, Key).

range_fold(Fun, Acc, Reader, Range) ->
    hanoidb_reader:range_fold(Fun, Acc, Reader, Range).

close_random_reader(Reader) ->
    hanoidb_reader:close(Reader).



open_batch_reader(Name, Options) ->
    hanoidb_reader:open(Name, [sequential|Options]).

read_next(Reader) ->
    case hanoidb_reader:next_node(Reader) of
        {node, KVs} ->
            {[ unfold(KV) || KV <- KVs], Reader};
        end_of_data ->
            'done';
        {error, _}=Err ->
            Err
    end.

unfold({Key,{Value, Expiry}}) when is_binary(Value); ?TOMBSTONE =:= Value ->
    {Key,Value,Expiry};
unfold({Key,Value}) ->
    {Key, Value, infinity}.

close_batch_reader(Reader) ->
    hanoidb_reader:close(Reader).

open_batch_writer(Name, Options) ->
    hanoidb_writer:init([Name, Options]).

write_next( {Key, Value, infinity}, Writer) ->
    {noreply, Writer2} = hanoidb_writer:handle_cast({add, Key, Value}, Writer),
    {ok, Writer2};
write_next( {Key, Value, Expiry}, Writer) ->
    {noreply, Writer2} = hanoidb_writer:handle_cast({add, Key, {Value, Expiry}}, Writer),
    {ok, Writer2}.

write_count( Writer ) ->
    case hanoidb_writer:handle_call(count, self(), Writer) of
        {ok, Count, _} ->
            {ok, Count};
        Err ->
            {error, Err}
    end.

close_batch_writer(Writer) ->
    {stop, normal, ok, _} = hanoidb_writer:handle_call(close, self(), Writer),
    ok.
