function(doc) {
  if (doc['type'] == 'job') {
    var request = doc['task'].split('/')[1]
    for (var inputFileIndex in doc['inputfiles']) {
      emit([request, doc['inputfiles'][inputFileIndex]['lfn']], doc['jobid']);
    }
  }
}
