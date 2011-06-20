function(doc) {
  if (doc.state == "ReadyToReject"){
     emit(doc._id, {"request" : doc.request.request_id});
  }
}