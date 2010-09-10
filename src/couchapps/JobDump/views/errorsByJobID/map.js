function(doc) {
  if (doc['type'] == 'fwjr') {
    for (var stepName in doc['fwjr'].steps) {
      if (doc['fwjr']['steps'][stepName].errors.length > 0) {
        emit([doc['jobid'], doc['retrycount']], 
             {'retry': doc['retrycount'],
              'step': stepName,
              'task': doc['fwjr']['task'],
              'error': doc['fwjr']['steps'][stepName].errors});
      }
    }
  }
}
