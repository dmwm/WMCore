function(doc) {
  // specific to wmagent view.
  if (doc.workflow) {
    emit(doc.workflow,  {'id': doc._id, 'rev': doc._rev}); 
  }
};
