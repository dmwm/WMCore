function(doc) {
   if (doc.owner){
        emit([doc.owner.group, doc.owner.user], { '_id' : doc._id, '_rev': doc._rev});
       } 
}