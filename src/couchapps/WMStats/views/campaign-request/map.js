
function(doc) {
  if (doc.type == "reqmgr_request"){
    emit(doc.campaign, {"id": doc._id}) ;
  } 
}