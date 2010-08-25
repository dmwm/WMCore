function (doc) {
    if (doc.user){
        emit([doc._id], doc.user.name)
    }
}
