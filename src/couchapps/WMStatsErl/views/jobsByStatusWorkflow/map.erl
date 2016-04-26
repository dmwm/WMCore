fun({Doc}) ->
  Id = couch_util:get_value(<<"_id">>, Doc),
  Rev = couch_util:get_value(<<"_rev">>, Doc),
  DocType = couch_util:get_value(<<"type">>, Doc),
  Workflow = couch_util:get_value(<<"workflow">>, Doc),
  Task = couch_util:get_value(<<"task">>, Doc),
  State = couch_util:get_value(<<"state">>, Doc),
  ExitCode = couch_util:get_value(<<"exitcode">>, Doc),
  Site = couch_util:get_value(<<"site">>, Doc),
  AcdcUrl = couch_util:get_value(<<"acdc_url">>, Doc),
  AgentName = couch_util:get_value(<<"agent_name">>, Doc),

  case DocType of
    undefined -> ok;
    <<"jobsummary">> ->
      Errors = couch_util:get_value(<<"errors">>, Doc),
      case Errors of
        undefined -> ok;
        {E} ->
          ListErrors = lists:sort(lists:foldl(fun(Step, StepAcc) ->
            {_StepKey, StepVal} = Step,
            StepAcc ++ lists:foldl(fun(Out, OutAcc) ->
               {O} = Out,
               Type = couch_util:get_value(<<"type">>, O),
               OutAcc ++ [Type]
            end, [], StepVal)
          end, [], E)),
          Emit([Workflow, Task, State, ExitCode, Site, AcdcUrl, AgentName, ListErrors], {[{id,Id},{rev,Rev}]})
      end;
    _ -> ok
  end
end.
