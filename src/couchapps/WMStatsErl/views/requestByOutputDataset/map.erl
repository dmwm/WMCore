fun({Doc}) ->
  OutDataSets = couch_util:get_value(<<"outputdatasets">>, Doc),
  case OutDataSets of
    undefined -> ok;
    _ ->
      lists:foreach(fun(DatasetName) -> Emit(DatasetName, null) end, OutDataSets)
    end
end.
