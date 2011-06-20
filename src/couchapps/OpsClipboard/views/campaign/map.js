function(doc) {
    if (doc.request.campaign_id) {
        emit([doc.request.campaign_id ], { "doc" : doc._id, "state" : doc.state, "updated" : doc.timestamp, "request" : doc.request.request_id});
    }
}