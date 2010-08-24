function(doc) {
  if (doc.type){
     if (doc.type == "config"){
        emit([doc.owner_id], { "config_doc" : doc._id, "config_label" : doc.config_label});
     }
  }
}