function(doc) {
  if (doc.request_status){
    emit(doc.request_status[doc.request_status.length - 1].status, null);
  }
}