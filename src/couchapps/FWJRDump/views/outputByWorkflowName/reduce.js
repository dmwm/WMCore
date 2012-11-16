function (key, values, rereduce) {
  var output = {'size': 0, 'events': 0, 'count': 0, 'dataset': null, 'tasks': {}};

  for (var someValue in values) {
    output['dataset'] = values[someValue]['dataset'];
    output['size'] += values[someValue]['size'];
    output['events'] += values[someValue]['events'];

    if (rereduce) {
      output['count'] += values[someValue]['count'];
    }
    else {
      output['count'] += 1;
    }

    if (rereduce) {
      for (var task in values[someValue]['tasks']) {
          output['tasks'][task] = true
      }
    }
    else {
      output['tasks'][values[someValue]['task']] = true
    }
  }

  return output;
}
