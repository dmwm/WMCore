
function(doc) {
  if (doc.type == "jobsummary") {
     emit([doc.workflow, doc.state, doc.exitcode], {"id": doc.id})
  } 
}