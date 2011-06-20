function(doc) {
  
  emit(doc.request.request_id, doc._id);
  
}