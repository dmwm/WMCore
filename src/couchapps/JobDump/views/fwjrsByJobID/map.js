function(doc) {
  if (doc['type'] == 'fwjr') {
    emit(doc['jobid'], {'_id': doc['_id']});
  }
}
