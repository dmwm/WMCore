function(doc) {
  if (doc['type'] == 'fwjr') {
    if (doc['fwjr'].task == null) {
      return;
    }

    for (var stepName in doc['fwjr'].steps) {
      if (stepName.search('cmsRun') == -1) {
        continue;
      }

      if (doc['fwjr']['steps'][stepName].performance) {
        emit([doc['jobid'], doc['retrycount']], 
             doc['fwjr']['steps'][stepName].performance);
      }
    }
  }
}
