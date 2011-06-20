function(doc) {
  emit([doc.request.request_id], { "doc" : doc._id, "state" : doc.state, "updated" : doc.timestamp} );
}