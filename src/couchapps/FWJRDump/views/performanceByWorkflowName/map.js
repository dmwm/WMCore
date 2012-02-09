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
      var multi = doc['fwjr']['steps'][stepName]['performance']['multicore'];
      var perfInfo = Object();

      perfInfo['jobID'] = doc['jobid']
      perfInfo['retry_count'] = doc['retrycount']
      perfInfo['taskName'] = doc['fwjr'].task
      perfInfo['stepName'] = stepName

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
      if (mem.PeakValuePss) {
          perfInfo['PeakValuePss'] = mem['PeakValuePss'];
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

      //Do loop over multicore info
      if (multi) {
	for (var perfName in multi) {
	  if (Number(multi[perfName]) != Number.NaN) {
	    perfInfo[perfName] = multi[perfName]
	  }
	  else {
	    perfInfo[perfName] = 0.0
	  }
	}
      }//END loop over multi
      emit([specName], perfInfo);
    }
  }
}