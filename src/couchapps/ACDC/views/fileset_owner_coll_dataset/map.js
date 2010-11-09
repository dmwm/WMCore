function(doc) {
    if (doc.fileset) {
        emit([doc.owner.group, doc.owner.user, doc.fileset.collection_id,doc.fileset.dataset], doc._id);
    };
}