function(doc) {
  for (var lfn in doc.files) {
    filesetFile = doc.files[lfn];
    filesetFile["locations"].sort();
    emit([doc.owner.group, doc.owner.user, doc.collection_name, doc.fileset_name, 
          filesetFile["locations"], filesetFile["lfn"]], filesetFile);
  }
}
