function(doc) {
  if(doc.RequestStatus == "new") {
    emit(null, doc);
  }
}
