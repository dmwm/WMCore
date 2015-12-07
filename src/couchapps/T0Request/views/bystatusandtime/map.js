function(doc) {
  var updateTime = null;
  if (doc.RequestTransition) {
  	updateTime = doc.RequestTransition[doc.RequestTransition.length - 1].UpdateTime;
  }
  emit([doc.RequestStatus, updateTime], null);
}