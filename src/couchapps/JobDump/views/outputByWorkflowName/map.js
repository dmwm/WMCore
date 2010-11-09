function(doc) {
  if (doc['type'] == 'fwjr') {
    if (doc['fwjr'].task == null) {
      return;
    }

    var specName = doc['fwjr'].task.split('/')[1]

    var cmsRunFound = false;
    for (var stepName in doc['fwjr']['steps']) {
      if (stepName == 'cmsRun1') {
        cmsRunFound = true;
      }

      if (doc['fwjr']['steps'][stepName].status != 0) {
        return;
      }
    }

    if (cmsRunFound) {
      var stepOutput = doc['fwjr']['steps']['cmsRun1']['output']
      for (var outputModuleName in stepOutput) {
        for (var outputFileIndex in stepOutput[outputModuleName]) {
          var outputFile = stepOutput[outputModuleName][outputFileIndex];

          if (outputModuleName == 'Merged' || outputFile.merged) {
            var datasetPath = '/' + outputFile['dataset']['primaryDataset'] +
                              '/' + outputFile['dataset']['processedDataset'] +
                              '/' + outputFile['dataset']['dataTier'];
            emit([specName, datasetPath], {'size': outputFile['size'],
                                           'events': outputFile['events'],
                                           'dataset': datasetPath});
          }
        }
      }
    }
  }
}
