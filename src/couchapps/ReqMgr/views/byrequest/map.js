function(doc) {
  emit(doc.RequestName, [doc.RequestStatus, doc.RequestType]);
}