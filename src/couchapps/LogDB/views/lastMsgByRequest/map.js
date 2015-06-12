function(doc) {
  if(doc.type) {
    var objs = doc.messages;
    emit(doc.request, [doc.type, doc.thr, objs[objs.length - 1]]);
  }
}
