// 'request' view
function(doc) 
{
	emit([doc.request.request_id], { "doc_id" : doc._id, "state" : doc.state, "updated": doc.timestamp});
}