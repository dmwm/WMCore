function(doc) {
  if (doc.filelist) {
    for (var filelistFile in doc.filelist.files) {
      emit([doc.filelist.collection_id, doc.filelist.task, doc.filelist.fileset_id, doc._id, filelistFile], doc.filelist.files[filelistFile]);
    }
  }
}