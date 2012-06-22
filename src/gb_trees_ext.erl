
-module(gb_trees_ext).
-extends(gb_trees).
-export([fold/3]).

% author: http://erlang.2086793.n4.nabble.com/gb-trees-fold-td2228614.html

-spec fold(fun((term(), term(), term()) -> term()), term(), gb_tree()) -> term().
fold(F, A, {_, T})
  when is_function(F, 3) ->
    fold_1(F, A, T).

fold_1(F, Acc0, {Key, Value, Small, Big}) ->
    Acc1 = fold_1(F, Acc0, Small),
    Acc = F(Key, Value, Acc1),
    fold_1(F, Acc, Big);
fold_1(_, Acc, _) ->
    Acc.
