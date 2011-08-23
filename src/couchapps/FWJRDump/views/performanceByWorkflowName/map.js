function(doc) {
  if (doc['type'] == 'fwjr') {
    if (doc['fwjr'].task == null) {
      return;
    }

    var specName = doc['fwjr'].task.split('/')[1]

    for (var stepName in doc['fwjr']['steps']) {
      var CPU = doc['fwjr']['steps'][stepName]['performance']['cpu'];
      var mem = doc['fwjr']['steps'][stepName]['performance']['memory'];
      var store = doc['fwjr']['steps'][stepName]['performance']['storage'];
      var perfInfo = Object();

      perfInfo['jobID'] = doc['jobid']
      perfInfo['retry_count'] = doc['retrycount']
      perfInfo['taskName'] = doc['fwjr'].task

      if (CPU && CPU.TotalJobCPU) {
	for (var perfName in CPU) {
	  if (Number(CPU[perfName]) != Number.NaN) {
	    perfInfo[perfName] = CPU[perfName]
	  }
	  else {
	    perfInfo[perfName] = 0.0
	  }
	}

      }//END if loop over CPU
      if (mem.PeakValueRss) {
	perfInfo['PeakValueRss'] = mem['PeakValueRss'];
      }
      if (mem.PeakValueVsize) {
	perfInfo['PeakValueVsize'] = mem['PeakValueVsize'];
      }

      //Do start/stop times
      perfInfo['startTime'] = doc['fwjr']['steps'][stepName]['start'];
      perfInfo['stopTime'] = doc['fwjr']['steps'][stepName]['stop'];

      if (store) {
	for (var perfName in store) {
	  if (Number(store[perfName]) != Number.NaN) {
	    perfInfo[perfName] = store[perfName]
	  }
	  else {
	    perfInfo[perfName] = 0.0
	  }
	}
      }//END if loop over storage
      emit([specName], perfInfo);
    }
  }
}