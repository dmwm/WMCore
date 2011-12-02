// 'release' view
// return requests in the required state
function(doc) 
{
	if (doc.state == "ReadyToRelease")
	{
		emit(doc._id, {"request_id" : doc.request.request_id});
	}
}