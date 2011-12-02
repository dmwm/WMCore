// 'campaign_ids' view
function(doc)
{
	if (doc.request.campaign_id) 
	{	
		// campaign_id is campaign name
		emit(doc.request.campaign_id, null);
    }
}