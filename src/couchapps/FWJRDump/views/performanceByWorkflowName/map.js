function(doc) {
  if (doc['type'] == 'fwjr') {
    if (doc['fwjr'].task == null) {
      return;
    }

    var specName = doc['fwjr'].task.split('/')[1]

    for (var stepName in doc['fwjr']['steps']) {
      var CPU = doc['fwjr']['steps'][stepName]['performance']['cpu'];
      var perfInfo = Object();

      if (CPU && CPU.TotalJobCPU) {
	for (var perfName in CPU) {
	  if (Number(CPU[perfName]) != Number.NaN) {
	    perfInfo[perfName] = CPU[perfName]
	  }
	}
	emit([specName], perfInfo);
      }
      else {
	continue;
      }
    }
  }
}