function(doc) {
  if (doc['type'] == 'fwjr') {
    if (doc['fwjr'].task == null) {
      return;
    }

    var specName = doc['fwjr'].task.split('/')[1]
    var taskName = doc['fwjr'].task

    var cmsRunFound = false;
    for (var stepName in doc['fwjr']['steps']) {
      if (doc['fwjr']['steps'][stepName].status != 0) {
        return;
      }
    }
    
    var wrappedTotalJobTime = 0;
    var cmsRunCPUPerformance = {
        totalJobCPU: 0,
        totalJobTime: 0,
        totalEventCPU: 0,
    }
    var datasetData = {};
    var inputEvents = 0;
    for (var stepName in doc['fwjr']['steps']) {
      var stepObj = doc['fwjr']['steps'][stepName];
      var site = stepObj.site;
      wrappedTotalJobTime += stepObj.stop - stepObj.start;
      
      // check whether it is cmsRun step
      if (stepName.indexOf("cmsRun") !== -1) {
          
          var stepOutput = stepObj.output;
          for (var outputModuleName in stepOutput) {
            for (var outputFileIndex in stepOutput[outputModuleName]) {
              var outputFile = stepOutput[outputModuleName][outputFileIndex];
              // only calculate merged files.
              if (outputFile.merged){
                  var datasetPath = '/' + outputFile['dataset']['primaryDataset'] +
                                      '/' + outputFile['dataset']['processedDataset'] +
                                      '/' + outputFile['dataset']['dataTier'];
                  var totalLumis = 0;
                  for (var run in outputFile['runs']) {
                      totalLumis += outputFile['runs'][run].length;
                  }
                  datasetData[datasetPath] = {'size': outputFile['size'], 
                                              'events': outputFile['events'],
                                              'totalLumis': totalLumis}
              }
            }
          }
          cmsRunCPUPerformance.totalJobCPU += Number(stepObj.performance.cpu.TotalJobCPU);
          cmsRunCPUPerformance.totalJobTime += Number(stepObj.performance.cpu.TotalJobTime);
          cmsRunCPUPerformance.totalEventCPU += Number(stepObj.performance.cpu.TotalEventCPU);
          
          if (stepObj.input) {
              for (var index in stepObj.input.source) {
                  inputEvents += stepObj.input.source[index].events;
              }
          }
      }
    }
    emit([specName, taskName, site], {'wrappedTotalJobTime': wrappedTotalJobTime,
                                      'cmsRunCPUPerformance': cmsRunCPUPerformance,
                                      'inputEvents': inputEvents,
                                      'dataset': datasetData});
  }
}
