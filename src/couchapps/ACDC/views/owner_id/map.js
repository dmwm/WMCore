function(doc) {
    if (doc.owner) {
        emit(doc._id, { 'id': doc._id, 'owner' : doc.owner} )
    }
}