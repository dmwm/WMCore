function(doc) {
  if(doc.RequestStatus == "approved") {
    emit(null, doc);
  }
}
