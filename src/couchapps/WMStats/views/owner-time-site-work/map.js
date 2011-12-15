function(doc) {
  for (site in doc.sites) {
    emit([doc.owner, doc.timestamp, site, doc.workload], doc.sites[site]);
  }
}