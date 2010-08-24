function (doc) {
    if (doc.user){
        emit([doc.user.group, doc.user.name], doc._id)
    }
}
