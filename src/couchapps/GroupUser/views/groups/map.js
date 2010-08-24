function (doc) {
    if (doc.group){
        emit([doc._id], doc.group.name)
    }
}
