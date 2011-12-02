function(doc) {
  if (doc.status){
    emit([doc.owner, doc.timestamp], doc.status)
  }
}