function(doc) {
  if (doc.type == "config") {
    emit(doc.id, {"id": doc.id});
  }
}
