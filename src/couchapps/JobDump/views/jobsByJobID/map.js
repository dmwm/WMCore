function(doc) {
  if (doc['type'] == 'job') {
    emit(doc['jobid'], {'_id': doc['_id']});
  }
}
