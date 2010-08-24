function(doc) {
    if (doc.fileset) {
        emit([doc.fileset.owner.group, doc.fileset.owner.user, doc.fileset.collection_id,doc.fileset.dataset], doc._id);
    };
}