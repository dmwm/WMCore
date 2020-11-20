function(doc) {
  emit([doc.RequestStatus, doc.RequestType, doc.Requestor], null);
}