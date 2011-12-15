function(doc) {
  if (doc.status){
    emit([doc.owner, doc.workload, doc.timestamp], doc.status)
  }
}