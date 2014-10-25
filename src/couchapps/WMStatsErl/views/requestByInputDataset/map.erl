fun({Doc}) ->
  DocInput = couch_util:get_value(<<"inputdataset">>, Doc),
  case DocInput of
    undefined -> ok;
    <<"">> -> ok;
    _ -> Emit(DocInput, null)
  end
end.
