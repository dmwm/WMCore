function(doc) {
   if (doc.collection){
        emit([doc.collection.owner_id], { 'collection_id' :doc._id, 'name': doc.collection.name, 'owner_id' : doc.collection.owner_id });
   }
}