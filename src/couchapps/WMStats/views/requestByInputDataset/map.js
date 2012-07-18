
function(doc) {
  if (doc.type == "reqmgr_request"){
    emit([doc.inputdataset, doc._id], {"id": doc._id}) ;
  } 
}