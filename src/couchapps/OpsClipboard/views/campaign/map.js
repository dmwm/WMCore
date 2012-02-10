// 'campaign' view
function(doc) 
{
	if (doc.request.campaign_id) 
	{
		emit(doc.request.campaign_id, { "doc_id" : doc._id,
			                            "state" : doc.state,
			                            "updated" : doc.timestamp,
			                            "request_id" : doc.request.request_id });
    }
}