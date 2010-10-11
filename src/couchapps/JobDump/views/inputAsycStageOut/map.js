function complete_job(doc, req) {
  for (step in doc['fwjr']['steps']) {
    if (doc['fwjr']['steps'][step]['status'] != 0) {
      return false;
    }
  }
  return true;
}


function(doc) {
  if (doc.type == "fwjr" && doc['fwjr'] && doc['fwjr']['steps'] && doc['fwjr']['asynDestination'] && doc['fwjr']['asynSource'] && doc['fwjr']['task'] && doc['fwjr']['userDN'] && complete_job(doc)) {
  var dn = doc['fwjr']['userDN'] || 'Fred';
  var task = doc['fwjr']['task'] || 0;
  var job = doc['jobid'];
    for (step in doc['fwjr']['steps']) {
      for (module in doc['fwjr']['steps'][step]["output"]) {
        for (file in doc['fwjr']['steps'][step]["output"][module]) {
          emit(doc['timestamp'],
                {'dn' : dn,
                 'task' : task,
                 'jobid' : job,
                 '_id': doc['fwjr']['steps'][step]["output"][module][file]["lfn"],
                 'source':  doc['fwjr']['asynSource'],
                 'destination': doc['fwjr']['asynDestination']}
          );  
        }
      }
    }
  }
}
