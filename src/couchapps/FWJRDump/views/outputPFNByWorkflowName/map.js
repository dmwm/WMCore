function(doc) {
  if (doc['type'] == 'fwjr') {
    if (doc['fwjr'].task == null) {
      return;
    }

    var request = doc['fwjr'].task.split('/')[1]
    var res = ''

    for (var stepName in doc['fwjr']['steps']) {
      var stepOutput = doc['fwjr']['steps'][stepName]['output']
      for (var outputModuleName in stepOutput) {
        if (outputModuleName == 'logArchive') {
          continue;
        };
        for (var outputFileIndex in stepOutput[outputModuleName]) {
          var outputFile = stepOutput[outputModuleName][outputFileIndex];
          if (outputFile['OutputPFN'] != '') {
            res = {"jobid" : doc['jobid'], "pfn" : outputFile['OutputPFN'], "checksums" : outputFile['checksums']};
          }
        }
      }
    }
   if (res != '') {
    emit([request], res);
    }
  }
}
