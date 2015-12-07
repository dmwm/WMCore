fun({Doc}) ->
  DocType = couch_util:get_value(<<"type">>, Doc),
  case DocType of
    undefined -> ok;
    <<"agent_request">> ->
      Timestamp = couch_util:get_value(<<"timestamp">>, Doc),
      Workflow = couch_util:get_value(<<"workflow">>, Doc),
      Emit([Timestamp, Workflow], null);
    _ -> ok
  end
end.
