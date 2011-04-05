function(keys, values) {
  var reducedValue = {RequestName:"None", Jobs: 0, Team: "None", CompleteJobs: 0};
  for (var i =0; i < values.length; i++){
      reducedValue.RequestName = values[i].RequestName
      reducedValue.Jobs += values[i].Jobs;
      reducedValue.Team = values[i].Team;
      reducedValue.CompleteJobs += values[i].CompleteJobs;
  };
  return reducedValue;
}