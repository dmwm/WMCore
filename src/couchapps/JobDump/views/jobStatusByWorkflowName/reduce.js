function (key, values, rereduce) {
  var output = {'jobid': 0, 'timestamp': 0, 'state': 0};

  for (var someValue in values) {
    if (values[someValue]['timestamp'] > output['timestamp']) { 
      output = values[someValue];
    }
  }

  return output;
}
