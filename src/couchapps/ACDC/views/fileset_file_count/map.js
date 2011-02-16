function(doc) {
  if (doc.filelist) {
    for (var filelistFile in doc.filelist.files) {
      emit([doc.filelist.fileset_id], 1);
    }
  }
}
