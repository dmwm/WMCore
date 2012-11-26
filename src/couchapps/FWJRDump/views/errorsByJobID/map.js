function(doc) {
  if (doc['type'] == 'fwjr') {
    if (doc['fwjr'].task == null) {
      return;
    }

    var specName = doc['fwjr'].task.split('/')[1];
    var startTime = 99999999999;
    var stopTime = 0;

    for (var stepName in doc['fwjr'].steps) {
      var site = doc['fwjr'].steps[stepName].site
      startTime = Math.min(startTime, doc['fwjr']['steps'][stepName].start)
      stopTime = Math.max(stopTime, doc['fwjr']['steps'][stepName].stop)
      if (doc['fwjr']['steps'][stepName].errors.length > 0) {
        inputLFNs = new Array();
        inputRuns = new Object;
	logLFNs   = new Array();

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
        } //END for loop over inputs

	if ('logArch1' in doc['fwjr'].steps) {
	  for (var outputName in doc['fwjr']['steps']['logArch1']['output']) {
	    for (var outputIndex in doc['fwjr']['steps']['logArch1']['output'][outputName]) {
	      logLFNs.push(doc['fwjr']['steps']['logArch1']['output'][outputName][outputIndex]['lfn']);
	    } //END loop over log archive outputs
	  }//END loop over output names
	}

        emit([doc['jobid'], doc['retrycount']], 
             {'jobid': doc['jobid'],
              'retry': doc['retrycount'],
              'step': stepName,
              'task': doc['fwjr']['task'],
              'error': doc['fwjr']['steps'][stepName].errors,
              'input': inputLFNs,
              'runs': inputRuns,
	      'logs': logLFNs,
	      'start': startTime,
	      'stop': stopTime,
	      'site': site});
      }
    } //END for loop over steps
  }
}
