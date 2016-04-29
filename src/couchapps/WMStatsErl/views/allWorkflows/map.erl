fun({Doc}) ->
  Workflow = couch_util:get_value(<<"workflow">>, Doc),
  case Workflow of
    undefined -> ok;
    _ ->
      Id = couch_util:get_value(<<"_id">>, Doc),
      Rev = couch_util:get_value(<<"_rev">>, Doc),
      Emit(Workflow, {[{id,Id},{rev,Rev}]})
  end
end.
