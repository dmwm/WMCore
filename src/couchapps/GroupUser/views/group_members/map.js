function(doc) {
  if (doc.user){
     emit([doc.user.group], { "id" : doc._id, "user" : doc.user.name });
  }
}