function(doc) {
  if (doc['type'] == 'job') {
    var jobid = parseInt(doc['jobid'])
    emit(jobid)
  }
}