fun({Doc}) ->
  DocType = couch_util:get_value(<<"type">>, Doc),
  case DocType of
    undefined -> ok;
    <<"agent_request">> ->
      AgentUrl = couch_util:get_value(<<"agent_url">>, Doc),
      Rev = couch_util:get_value(<<"_rev">>, Doc),
      Emit(AgentUrl, {[{rev,Rev}]});
    _ -> ok
  end
end.
