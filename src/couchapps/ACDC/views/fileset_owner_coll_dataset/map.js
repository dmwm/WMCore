function(doc) {
    if (doc.fileset) {
        emit([doc.fileset.owner_id, doc.fileset.collection_id,doc.fileset.dataset], doc._id);
    };
}