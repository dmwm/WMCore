function(doc) {
  if (doc.type == "jobsummary") {
    if (doc.state == "jobcooloff") {
        emit(doc.workflow, null);
    }
  }
}
