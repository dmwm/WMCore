function(doc) {
  if (doc['type'] == 'fwjr') {
    if (doc['fwjr'].task == null) {
      return;
    }

    for (var stepName in doc['fwjr'].steps) {
      if (doc['fwjr']['steps'][stepName].performance) {
        emit([doc['jobid'], doc['retrycount'], doc['timestamp']], 
             [stepName, doc['fwjr']['steps'][stepName].start,
              doc['fwjr']['steps'][stepName].performance,
              doc['fwjr']['steps'][stepName].errors]);
      }
    }
  }
}
