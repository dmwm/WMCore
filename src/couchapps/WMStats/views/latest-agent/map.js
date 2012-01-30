function(doc) {
  if (doc.type == "agent_request") {
    if (doc.timestamp) {
      emit(doc.workflow, {"timestamp": doc.timestamp, "id": doc._id});
    }
  }
}
