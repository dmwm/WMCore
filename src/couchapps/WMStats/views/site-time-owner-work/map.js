function(doc) {
  for (site in doc.sites) {
    emit([site, doc.timestamp, doc.owner, doc.workload], doc.sites[site]);
  }
}