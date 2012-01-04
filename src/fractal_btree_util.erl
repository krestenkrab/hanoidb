-module(fractal_btree_util).

-compile(export_all).


index_file_name(Name) ->
    Name.

estimate_node_size_increment(_KVList,Key,Value) ->
    byte_size(Key)
        + 10
        + if
              is_integer(Value) ->
                  5;
              is_binary(Value) ->
                  5 + byte_size(Value);
              is_atom(Value) ->
                  8
          end.

encode_index_node(Level, KVList) ->
    Data = %zlib:zip(
             erlang:term_to_binary({Level, KVList})
           % )
        ,
    Size = byte_size(Data),
    {ok, Size+4, [ <<Size:32>> | Data ] }.

decode_index_node(Data) ->
    {Level,KVList} = erlang:binary_to_term(Data), %zlib:unzip(Data)),
    {ok, {node, Level, KVList}}.
