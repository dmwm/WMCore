function(doc) {  
    if (doc.fileset) {
        if (doc.fileset.task){
            emit([doc.fileset.collection_id, doc.fileset.task], doc._id);
        }
    }
}