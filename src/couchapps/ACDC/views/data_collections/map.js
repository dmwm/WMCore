function(doc) {  
    if (doc.collection) {
        if (doc.collection.collection_type == "ACDC.CollectionTypes.DataCollection"){
            emit(doc.collection.name, { '_id': doc._id, 'collection' : doc.collection, 'owner' : doc.owner} )
        }
    }
}