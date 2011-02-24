function(doc) {
  if (doc.filelist) {
    for (var filelistLFN in doc.filelist.files) {
      filelistFile = doc.filelist.files[filelistLFN];
      var totalLumis = 0;
      for (var runIndex in filelistFile["runs"]) {
        totalLumis += filelistFile["runs"][runIndex].lumis.length;
      }
      filelistFile["locations"].sort();
      emit([doc.filelist.collection_id, doc.filelist.task, filelistFile["locations"], filelistFile["lfn"]],
           {"lfn": filelistFile["lfn"], "events": filelistFile["events"], "lumis": totalLumis});
    }
  }
}