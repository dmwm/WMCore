function complete_job(doc, req) {
  for (step in doc['fwjr']['steps']) {
    if (doc['fwjr']['steps'][step]['status'] != 0) {
      return false;
    }
  }
  return true;
}


function(doc) {
  if (doc.type == "fwjr" && doc['fwjr'] && doc['fwjr']['steps'] && doc['fwjr']['task'] && complete_job(doc)) {
  var task = doc['fwjr']['task'] || 0;
  var job = doc['jobid'];
    for (step in doc['fwjr']['steps']) {
      for (module in doc['fwjr']['steps'][step]["output"]) {
        for (file in doc['fwjr']['steps'][step]["output"][module]) {
          if (doc['fwjr']['steps'][step]["output"][module][file]['user_dn'] && doc['fwjr']['steps'][step]["output"][module][file]['async_dest']) {
          emit(doc['timestamp'],
                {'dn' : doc['fwjr']['steps'][step]["output"][module][file]['user_dn'],
                 'task' : task,
                 'jobid' : job,
                 '_id': doc['fwjr']['steps'][step]["output"][module][file]["lfn"],
                 'checksums': doc['fwjr']['steps'][step]["output"][module][file]['checksums'],
                 'size': doc['fwjr']['steps'][step]["output"][module][file]['size'],
                 'group' : doc['fwjr']['steps'][step]["output"][module][file]['user_vogroup'],
                 'role': doc['fwjr']['steps'][step]["output"][module][file]['user_vorole'],
                 'source' : doc['fwjr']['steps'][step]["output"][module][file]["location"],
                 'destination': doc['fwjr']['steps'][step]["output"][module][file]['async_dest']}
          );
       }

       }
      }
    }
  }
}

