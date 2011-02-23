function(doc) {
  if (doc['type'] == 'fwjr') {
    if (doc['fwjr'].task == null) {
      return;
    }

    var specName = doc['fwjr'].task.split('/')[1];

    for (var stepName in doc['fwjr'].steps) {
      if (doc['fwjr']['steps'][stepName].errors.length > 0) {
        inputLFNs = new Array();
        inputRuns = new Object;

        for (var inputName in doc['fwjr']['steps'][stepName]['input']) {
          if (inputName == 'source') {
            for (var inputIndex in doc['fwjr']['steps'][stepName]['input'][inputName]) {
              inputLFNs.push(doc['fwjr']['steps'][stepName]['input'][inputName][inputIndex]['lfn']);

              var inputSection = doc['fwjr']['steps'][stepName]['input'][inputName][inputIndex];
              for (var inputRun in inputSection['runs']) {
                var found = false;
                for (var knownRun in inputRuns) {
                  if (knownRun == inputRun) {
                    found = true;
                    break;
                  }
                }

                if (found == false) {
                  inputRuns[inputRun] = new Array();
                }
                
                for (var lumiIndex in inputSection['runs'][inputRun]) {
                  var found = false;
                  var inputLumi = inputSection['runs'][inputRun][lumiIndex];

                  for (var foundLumiIndex in inputRuns[inputRun]) {
                    if (inputLumi == inputRuns[inputRun][foundLumiIndex]) {
                      found = true;
                      break;
                    }
                  }

                  if (found == false) {
                    inputRuns[inputRun].push(inputLumi);
                  }
                }
              }
            }
          }
        }

        emit([doc['jobid'], doc['retrycount']], 
             {'jobid': doc['jobid'],
              'retry': doc['retrycount'],
              'step': stepName,
              'task': doc['fwjr']['task'],
              'error': doc['fwjr']['steps'][stepName].errors,
              'input': inputLFNs,
              'runs': inputRuns});
      }
    }
  }
}
