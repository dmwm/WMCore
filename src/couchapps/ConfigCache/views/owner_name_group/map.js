function(doc) {
  if (doc.owner){
     emit([doc.owner.group, doc.owner.name], doc._id);
  }
}