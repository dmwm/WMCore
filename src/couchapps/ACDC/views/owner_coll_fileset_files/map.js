function(doc) {
  for (var lfn in doc.files) {
    filesetFile = doc.files[lfn];
    var locationCopy = filesetFile["locations"].slice();
    locationCopy.sort();
    emit([doc.owner.group, doc.owner.user, doc.collection_name, doc.fileset_name, 
          locationCopy, filesetFile["lfn"]], filesetFile);
  }
}
