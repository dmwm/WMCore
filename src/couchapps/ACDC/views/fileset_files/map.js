function(doc) {
  if (doc.filelist) {
    for (var fileIndex in doc.filelist.files) {
      filelistFile = doc.filelist.files[fileIndex];
      filelistFile["locations"].sort();
      emit([doc.filelist.collection_id, doc.filelist.task, filelistFile["locations"], filelistFile["lfn"]], filelistFile);
    }
  }
}