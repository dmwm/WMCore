function(doc) {
  if (doc.type == "agent_request") {
    emit(doc.workflow, {"id": doc._id, "timestamp": doc.timestamp});
  }
}