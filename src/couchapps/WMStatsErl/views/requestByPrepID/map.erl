fun({Doc}) ->
  PrepID= couch_util:get_value(<<"prep_id">>, Doc),
  case PrepID of
    undefined -> ok;
    <<"">> -> ok;
    null -> ok;
    _ -> Emit(PrepID, null)
  end
end.
