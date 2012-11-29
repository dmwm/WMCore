function (key, values, rereduce) {
  var output = {'size': 0, 'events': 0, 'count': 0, 'dataset': null, 'tasks': []};

  var storedTasks = {}
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
      for (var i = 0; i < values[someValue]['tasks'].length; i++) {
        var task = values[someValue]['tasks'][i]
        if (!(task in storedTasks)){
          output['tasks'].push(task)
          storedTasks[task] = true
        }
      }
    }
    else {
      if (!(values[someValue]['task'] in storedTasks)){
        storedTasks[values[someValue]['task']] = true
        output['tasks'].push(values[someValue]['task'])
      }
    }
  }

  return output;
}
