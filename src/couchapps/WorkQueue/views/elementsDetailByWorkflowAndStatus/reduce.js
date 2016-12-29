function (key, values, rereduce) {
  var output = {'count': 0, 'totalJobs': 0};

  if (rereduce) {
    for (var someValue in values) {
      output['count'] += values[someValue]['count'];
      output['totalJobs'] += values[someValue]['totalJobs'];
    }
  } else {
  	output['count'] = values.length;
    for (var someValue in values) {
      output['totalJobs'] += values[someValue]['Jobs'];
    }
  }

  return output;
}