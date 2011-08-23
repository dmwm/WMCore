function(doc) {
  for (var lfn in doc.files) {
    filesetFile = doc.files[lfn];
    var totalLumis = 0;
    for (var runIndex in filesetFile["runs"]) {
      totalLumis += filesetFile["runs"][runIndex].lumis.length;
    }
    filesetFile["locations"].sort();
    emit([doc.owner.group, doc.owner.user, doc.collection_name, doc.fileset_name, 
          filesetFile["locations"], filesetFile["lfn"]],
         {"lfn": lfn, "events": filesetFile["events"], "lumis": totalLumis});
  }
}
