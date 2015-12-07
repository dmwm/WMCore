function(doc, site) {
    if (doc._attachments && doc._attachments.spec) {
        emit(doc._id, {'_id' : doc._id});
    }
}