function(doc) {
    if(doc.comments) {
        for(i=0;i<doc.comments.length;i++) {
            emit(doc.comments[i].ts, null);
        }
    }
}
