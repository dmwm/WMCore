function(doc) {
  emit([doc.owner.group, doc.owner.user, doc.collection_name, doc.fileset_name], null);
}