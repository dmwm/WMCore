function(doc) {
  if (doc['type'] == 'fwjr') {
    if (doc['fwjr'].task == null) {
      return;
    }

    var specName = doc['fwjr'].task.split('/')[1];

    for (var stepName in doc['fwjr'].steps) {
      if (doc['fwjr']['steps'][stepName].errors.length > 0) {
        emit([specName, doc['jobid'], doc['retrycount']], 
             {'jobid': doc['jobid'],
              'retry': doc['retrycount'],
              'step': stepName,
              'task': doc['fwjr']['task'],
              'error': doc['fwjr']['steps'][stepName].errors});
      }
    }
  }
}
