function(doc) {
  for (site in doc.sites) {
    emit([doc.timestamp, site, doc.owner, doc.workload], doc.sites[site]);
  }
}