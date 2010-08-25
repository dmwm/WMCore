function(doc) {  
    if (doc.collection) {
        emit([doc.collection.name, doc.collection.owner_id] , { '_id': doc._id, 'collection' : doc.collection} )
    }
}