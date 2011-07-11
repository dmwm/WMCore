function(doc) {
  if (doc['type'] == 'fwjr') {
    if (doc['fwjr'].task == null) {
      return;
    }

    var request = doc['fwjr'].task.split('/')[1]

    // Make sure every step completed
    var overallStatus = 0;
    for (var stepName in doc['fwjr']['steps']) {
      if (doc['fwjr']['steps'][stepName].status != 0) {
        return;
      }
    }

    // Find input sections, each of which is a vector
    for (var stepName in doc['fwjr']['steps']) {
      var stepInput = doc['fwjr']['steps'][stepName]['input']
      for (var inputSource in stepInput) {
        for (var sourceKey in stepInput[inputSource]) {
          // Find runs within FWJR, emit
          for (var field in stepInput[inputSource][sourceKey]) {
            if (field == 'runs') {
              emit(request, {"jobid" : doc['jobid'], "runs" : stepInput[inputSource][sourceKey]['runs'] });
            }
          }
        }
      }
    }
  }
}
