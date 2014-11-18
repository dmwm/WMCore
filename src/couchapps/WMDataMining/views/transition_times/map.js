function(doc) {
  if (doc.updateTime) {
    emit([doc.updateTime, doc._id], [doc.campaign, doc.outputTier, doc.status, doc.priority, doc.newTime, doc.approvedTime, doc.assignedTime, doc.acquireTime, doc.firstJobTime, doc.lastJobTime, doc.completedTime, doc.closeoutTime, doc.announcedTime, doc.updateTime ]);
  }
}
