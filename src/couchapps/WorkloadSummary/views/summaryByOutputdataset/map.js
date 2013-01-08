function(doc) {
    if (doc.output) {
        for (var dataset in doc.output) {
            emit(dataset, null)
        }
    }
}
