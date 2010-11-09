function(doc) {
   if (doc.fileset) {
       emit([doc._id] , { "_id": doc._id, 'fileset' : {"dataset" : doc.fileset.dataset, "collection_id" : doc.fileset.collection_id, "owner":  doc.owner} } )
   }
}