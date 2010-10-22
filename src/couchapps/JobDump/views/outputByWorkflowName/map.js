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

      if (doc['fwjr']['steps'][stepName].status != 0) {
        return;
      }

      var stepOutput = doc['fwjr']['steps'][stepName]['output']
      for (var outputModuleName in stepOutput) {
        for (var outputFileIndex in stepOutput[outputModuleName]) {
          var outputFile = stepOutput[outputModuleName][outputFileIndex];

          if (outputModuleName == 'Merged' || (outputFile.hasAttribute('merged') &&
                                               outputFile.getAttribute('merged'))) {
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
