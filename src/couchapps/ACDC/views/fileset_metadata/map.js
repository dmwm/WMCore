function(doc) {
  if (doc.filelist) {
    for (var filelistLFN in doc.filelist.files) {
      filelistFile = doc.filelist.files[filelistLFN];
      var totalLumis = 0;
      for (var run in filelistFile["runs"]) {
        totalLumis += run.Lumis.length;
      }
      emit([doc.filelist.collection_id, doc.filelist.task, doc.filelist.fileset_id, doc._id, filelistFile["lfn"]],
           {"lfn": filelistFile["lfn"], "events": filelistFile["events"], "lumis": totalLumis});
    }
  }
}