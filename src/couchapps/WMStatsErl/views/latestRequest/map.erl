fun({Doc}) ->
  DocType = couch_util:get_value(<<"type">>, Doc),
  case DocType of
    undefined -> ok;
    <<"agent_request">> ->
      Workflow = couch_util:get_value(<<"workflow">>, Doc),
      AgentUrl = couch_util:get_value(<<"agent_url">>, Doc),
      Id = couch_util:get_value(<<"_id">>, Doc),
      Timestamp = couch_util:get_value(<<"timestamp">>, Doc),
      Emit([Workflow, AgentUrl], {[{id,Id},{timestamp,Timestamp}]});
    _ -> ok
  end
end.
