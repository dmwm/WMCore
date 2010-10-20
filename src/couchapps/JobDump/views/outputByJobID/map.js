function(doc) {
  if (doc['type'] == 'fwjr') {
    if (doc['fwjr'].task == null) {
      return;
    }

    var specName = doc['fwjr'].task.split('/')[1]

    for (var stepName in doc['fwjr']['steps']) {
      if (stepName != 'cmsRun1') {
        continue;
      }

      var stepOutput = doc['fwjr']['steps'][stepName]['output']
      for (var outputModuleName in stepOutput) {
        for (var outputFileIndex in stepOutput[outputModuleName]) {
          var outputFile = stepOutput[outputModuleName][outputFileIndex];
          if (outputFile['lfn'] != '' && 'location' in outputFile) {
            emit(doc['jobid'], {'lfn': outputFile['lfn'], 'location': outputFile['location']});
          }
        }
      }
    }
  }
}
