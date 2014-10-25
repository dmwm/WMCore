fun(Keys, Values, ReReduce) ->
  case ReReduce of
    true -> lists:sum(Values);
    _ -> length(Values)
  end
end.
