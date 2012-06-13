
function(doc) {
  if (doc.type == "agent_request") {
     if (doc.sites) {
        for (var site in doc.sites) {
            emit([site, doc.agent_url, doc.workflow], doc.timestamp);
        }
     }
  } 
}