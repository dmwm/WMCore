function(doc) {
   if (doc.messages) {
   	   var numMsg = doc.messages.length;
       for(var i = 0; i < numMsg; i++) {
           emit(doc.request, {type:doc.type, agent: doc.identifier, thr:doc.thr,
                ts: doc.messages[numMsg - 1].ts});
   	   }	
   }
}
