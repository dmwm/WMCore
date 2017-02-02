function(doc) {
    if (doc.request && doc.type) {
        emit([doc.request, doc.type], null);
    }
}
