function(doc) {
   for(var i=0; i<doc.messages.length; i++) {
      emit(doc.request, {type:doc.type, thr:doc.thr,
           ts:doc.messages[i].ts, msg:doc.messages[i].msg});
   }
}
