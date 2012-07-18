
function(doc) {
  if (doc.type == "agent_request"){
      for (var i in doc.output_progress) {
          emit([doc.output_progress[i].dataset, doc.workflow], null) ;
      }
  }
}