function(doc) {
    if (doc.request) {
        emit(doc.request, doc);
    }
}
