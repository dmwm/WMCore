function(doc) {
  if (doc.type === "agent_request" && doc.agent_url) {
    // this condition check can be removed when there is no historical data is left. Not sure why this check is not working
    //if (doc['_id'].startsWith(doc.agent_url)) {
        emit(doc['_id'],  {'rev': doc['_rev']});
    //}
  }
}
