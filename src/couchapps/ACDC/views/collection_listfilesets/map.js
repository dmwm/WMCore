function(doc) {
     if (doc.fileset){
         emit([doc.fileset.collection_id], { "_id": doc._id, 'fileset' : {"dataset" : doc.fileset.dataset, "collection_id" : doc.fileset.collection_id, "owner_id":  doc.fileset.owner_id}});
     } 
}