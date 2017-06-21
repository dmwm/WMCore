function(doc) {
  if (doc['type'] === 'fwjr') {
    if (doc['fwjr'].task == null || doc['fwjr'].skippedFiles.length === 0 || 
        doc["jobtype"] !== "Merge" || doc["jobstate"] !== "success") {
      return;
    }
    var specName = doc['fwjr'].task.split('/')[1];
    var task = doc['fwjr'].task;
    var site = "SiteNotReported";
    if (doc['fwjr'].steps.cmsRun1 && doc['fwjr'].steps.cmsRun1.site) {
        site = doc['fwjr'].steps.cmsRun1.site;
    }
    emit([specName, task, site], {"skippedFiles": doc['fwjr'].skippedFiles.length});
   }
}