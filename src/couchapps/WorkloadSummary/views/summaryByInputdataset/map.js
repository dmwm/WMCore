function(doc) {
    if (doc.inputdatasets) {
        for (var i in doc.inputdatasets) {
            emit(doc.inputdatasets[i], null)
        }
    }
}