fun({Doc}) ->
  DocType = couch_util:get_value(<<"type">>, Doc),
  case DocType of
    undefined -> ok;
    <<"reqmgr_request">> ->
      Campaign = couch_util:get_value(<<"campaign">>, Doc),
      ReqDate = couch_util:get_value(<<"request_date">>, Doc),
      Emit([Campaign, ReqDate], null);
    _ -> ok
  end
end.
