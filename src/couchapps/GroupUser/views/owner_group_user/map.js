function(doc) {
  if (doc.owner){
     emit([doc.owner.group, doc.owner.user], { "id" : doc._id, "rev" : doc._rev});
  }
}