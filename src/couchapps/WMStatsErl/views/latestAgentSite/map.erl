fun({Doc}) ->
  DocType = couch_util:get_value(<<"type">>, Doc),
  case DocType of
    undefined -> ok;
    <<"agent_request">> ->
      Sites = couch_util:get_value(<<"sites">>, Doc),
      case Sites of
        undefined -> ok;
        {L} ->
          AgentUrl = couch_util:get_value(<<"agent_url">>, Doc),
          Workflow = couch_util:get_value(<<"workflow">>, Doc),
          Timestamp = couch_util:get_value(<<"timestamp">>, Doc),
          lists:foreach(fun(Site) -> Emit([element(1,Site), AgentUrl, Workflow], Timestamp) end, L);
        _ -> ok
      end;
    _ -> ok
  end
end.
