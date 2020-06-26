fun({Doc}) ->
  DocType = couch_util:get_value(<<"type">>, Doc),
  case DocType of
    undefined -> ok;
    <<"jobsummary">> ->
      State = couch_util:get_value(<<"state">>, Doc),
      case State of
        undefined -> ok;
        <<"jobcooloff">> ->
          Workflow = couch_util:get_value(<<"workflow">>, Doc),
          Emit(Workflow, null);
        _ -> ok
      end;
    _ -> ok
  end
end.
