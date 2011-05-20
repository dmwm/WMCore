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

      if (CPU && CPU.TotalJobCPU) {
	for (var perfName in CPU) {
	  if (Number(CPU[perfName]) != Number.NaN) {
	    perfInfo[perfName] = CPU[perfName]
	  }
	}

      }//END if loop over CPU
      if (mem.PeakValueRss) {
	perfInfo['PeakValueRss'] = mem['PeakValueRss'];
      }
      if (mem.PeakValueVsize) {
	perfInfo['PeakValueVsize'] = mem['PeakValueVsize'];
      }

      if (store) {
	for (var perfName in store) {
	  if (Number(store[perfName]) != Number.NaN) {
	    perfInfo[perfName] = store[perfName]
	  }
	}
      }//END if loop over storage
      emit([specName], perfInfo);
    }
  }
}