fun(Keys, Values, ReReduce) ->
  [H|T] = Values,
  lists:foldl(fun(Req, LatestReq) -> {[_, {<<"timestamp">>,LatestTimestamp}]} = LatestReq,
                                     {[_, {<<"timestamp">>,Timestamp}]} = Req, 
                                     if LatestTimestamp < Timestamp -> Req; true -> LatestReq end
              end, H, T)
end.
