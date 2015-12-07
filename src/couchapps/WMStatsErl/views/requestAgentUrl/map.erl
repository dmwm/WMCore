fun({Doc}) ->
  DocType = couch_util:get_value(<<"type">>, Doc),
  case DocType of
    undefined -> ok;
    <<"agent_request">> ->
      AgentUrl = couch_util:get_value(<<"agent_url">>, Doc),
      Workflow = couch_util:get_value(<<"workflow">>, Doc),
      Emit([Workflow, AgentUrl], null);
    _ -> ok
  end
end.
