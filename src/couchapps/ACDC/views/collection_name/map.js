function(doc) {  
    if (doc.collection) {
        emit([doc.collection.name, doc.owner.group, doc.owner.user] , { '_id': doc._id, 'collection' : doc.collection} )
    }
}