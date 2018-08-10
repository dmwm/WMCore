function(doc) {
  if (doc.ParentageResolved !== undefined){
    emit([doc.ParentageResolved, doc.RequestStatus], doc.ChainParentageMap);
  }
}