fun({Doc}) ->
  DocType = couch_util:get_value(<<"type">>, Doc),
  case DocType of
    undefined -> ok;
    <<"tier0_request">> ->
      Workflow = couch_util:get_value(<<"workflow">>, Doc),
      Emit(Workflow, null);
    _ -> ok
  end
end.
