function(doc) {
  if (doc.filelist) {
     emit([doc.filelist.fileset_id], doc._id);
  }
}