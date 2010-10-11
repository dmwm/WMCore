function complete_job(doc, req) {
  for (step in doc['fwjr']['steps']) {
    if (doc['fwjr']['steps'][step]['status'] != 0) {
      return false;
    }
  }
  return true;
}


function(doc) {
  if (doc.type == "fwjr" && doc['fwjr'] && doc['fwjr']['steps'] && complete_job(doc)) {
    emit(doc['timestamp'],
          {'dn' : doc['fwjr']['userDN'],
           'task' : doc['fwjr']['task'],
           'jobid' : doc['jobid'],
           'output': doc['fwjr']['steps']['cmsRun1']["output"]["output"][0]["guid"],
           'source': doc['fwjr']['steps']['cmsRun1']["output"]["output"][0]["location"],
           'destination': doc['fwjr']['asynDestination']}
    );
  }
}
