function (key, values, rereduce) {
  var output = {'pending': 0, 'running': 0, 'cooloff': 0, 'success': 0, 'failure': 0};

  if (rereduce) {
    for (var someValue in values) {
      output['pending'] += values[someValue]['pending'];
      output['running'] += values[someValue]['running'];
      output['cooloff'] += values[someValue]['cooloff'];
      output['success'] += values[someValue]['success'];
      output['failure'] += values[someValue]['failure'];
    }
  } else {
    for (var someValue in values) {
      if (values[someValue]['state'] != 'transitioning') {
        output[values[someValue]['state']] += 1;
      }
    }
  }

  return output;
}
