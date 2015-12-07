function(doc) {
  if (doc.type == "agent_request") {
    emit([doc.workflow, doc.agent_url], {"id": doc._id, "timestamp": doc.timestamp});
  }
}