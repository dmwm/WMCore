function(doc) {
  if (doc.campaign && doc.outputTier) {
    var ods;
    for (ods in doc.outputDatasets) {
      if ((doc.mcmApprovalTime || doc.mcmTotalEvents) &&  doc.mcmApprovalTime != 'Unknown') {
        emit([doc.mcmApprovalTime, doc._id], [doc.mcmApprovalTime, doc.mcmTotalEvents,  doc.outputDatasets[ods], doc.outputTier, doc.campaign]);
      }
    }
  }
}
