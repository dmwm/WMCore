// 'reject' view
// return requests in the required state
function(doc) 
{
	if (doc.state == "ReadyToReject")
	{
		emit(doc._id, {"request_id" : doc.request.request_id});
	}	
}