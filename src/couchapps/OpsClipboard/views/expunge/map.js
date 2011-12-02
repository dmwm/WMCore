// 'expunge' view
// output all requests ready to be expunged from OpsClipboard, 
// i.e. requests in OpsClipboard state "ReadyToRelease" or "ReadyToReject"
function(doc)
{
	if (doc.state == "ReadyToReject" || doc.state == "ReadyToRelease")
	{
		emit(doc["state"], {"doc_id": doc._id,
			                "request_id": doc.request.request_id,
			                "updated": doc.timestamp,
			                // put revision - couchapp will need to delete the document
			                // eventually, knowing revision avoids
			                // {"error":"conflict","reason":"Document update conflict."}
		                    "rev": doc._rev});
	}
}