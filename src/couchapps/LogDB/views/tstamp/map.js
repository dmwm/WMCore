function(doc) {
    if(doc.comments) {
        for(i=0;i<doc.messages.length;i++) {
            emit(doc.messages[i].ts, null);
        }
    }
}
