function(doc) {
  if (doc['type'] === 'fwjr') {
    if (doc['fwjr'].task == null || doc['fwjr'].skippedFiles.length === 0 || 
        doc["jobtype"] !== "Merge" || doc["jobstate"] !== "success") {
      return;
    }
    var specName = doc['fwjr'].task.split('/')[1];
    var task = doc['fwjr'].task;
    
    emit([specName, task], {"skippedFiles": doc['fwjr'].skippedFiles.length});
   }
}