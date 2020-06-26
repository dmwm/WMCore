fun({Doc}) ->
  DocType = couch_util:get_value(<<"type">>, Doc),
  case DocType of
    undefined -> ok;
    <<"agent_info">> ->
      AgentUrl = couch_util:get_value(<<"agent_url">>, Doc),
      Emit(AgentUrl, {Doc});
    _ -> ok
  end
end.
