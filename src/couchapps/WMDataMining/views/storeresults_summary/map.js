function(doc) {
  if (doc.type == 'StoreResults') {
    emit([doc.updateTime, doc._id], [doc.campaign, doc.outputTier, doc.type, doc.status, doc.priority, doc.totalEvents, doc.eventProgress, doc.newTime, doc.inputDataset, doc.outputDatasets, doc.filterEfficiency, doc.runWhiteList]);
  }
}
