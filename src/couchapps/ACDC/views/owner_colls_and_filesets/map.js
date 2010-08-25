function(doc) {
   if (doc.fileset){
        emit([doc.fileset.owner_id], { '_id' : doc._id, '_rev': doc._rev});
       } 
   if (doc.collection){     
        emit([doc.collection.owner_id], { '_id' : doc._id, '_rev': doc._rev});  
       }
}