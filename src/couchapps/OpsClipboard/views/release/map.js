function(doc) {
  if (doc.state == "ReadyToRelease"){
     emit(doc._id, {"request" : doc.request.request_id});
  }
}
