function(doc) {
  emit([doc.RequestStatus, doc.Requestor], null);
}