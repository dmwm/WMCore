function(doc) {
  emit([doc.RequestStatus, doc.RequestType], null);
}