function(doc) {
  for(i = 0; i < doc.state_changes.length; i++) {
    emit([doc.state_changes[i].newstate, doc.state_changes[i].timestamp], doc.name);
    }
}
