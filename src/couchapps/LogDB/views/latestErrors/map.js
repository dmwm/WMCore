function(doc) {
   var idx = doc.messages.length-1;
   emit(doc.request, {type:doc.type, thr:doc.thr,
        ts:doc.messages[idx].ts, msg:doc.messages[idx].msg});
}
