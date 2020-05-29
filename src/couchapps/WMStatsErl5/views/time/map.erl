fun({Doc}) ->
  DocType = couch_util:get_value(<<"type">>, Doc),
  Timestamp = couch_util:get_value(<<"timestamp">>, Doc),
  case (Timestamp /= undefined) and ((DocType == <<"agent_request">>) or (DocType == <<"agent">>)) of
    true ->
      Id = couch_util:get_value(<<"_id">>, Doc),
      Rev = couch_util:get_value(<<"_rev">>, Doc),
      Emit(Timestamp, {[{id,Id},{rev,Rev}]});
    false -> ok
  end
end.
