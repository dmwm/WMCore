function(doc) {
  if (doc["WMCore.WorkQueue.DataStructs.WorkQueueElement.WorkQueueElement"]) {
	  emit(null, {'_id' : doc['_id']});
  }
}