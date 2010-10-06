function(doc) {
   if (doc.collection){
        emit([doc.owner.group, doc.owner.user], { 'collection_id' :doc._id, 'name': doc.collection.name });
    }
}