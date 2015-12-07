function(doc) {
    if(doc.messages) {
        for(var i=0; i<doc.messages.length; i++) {
            emit(doc.messages[i].ts, doc.request);
        }
    }
}
