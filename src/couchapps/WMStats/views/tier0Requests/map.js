function(doc) {
  if (doc.type == "tier0_request"){
    emit(doc.workflow, null);
  }
}