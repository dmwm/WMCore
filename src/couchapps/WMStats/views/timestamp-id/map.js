function(doc) {
  emit(doc.timestamp, doc._id);
}