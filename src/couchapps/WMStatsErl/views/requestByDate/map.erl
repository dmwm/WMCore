fun({Doc}) ->
  DocDate = couch_util:get_value(<<"request_date">>, Doc),
  case DocDate of
    undefined -> ok;
    [H|T] ->
      Emit(DocDate, null);
    _ -> ok
  end
end.
