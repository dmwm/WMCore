function(doc){
    if (doc.request.campaign_id) {
        emit(doc.request.campaign_id, null)
    }
}