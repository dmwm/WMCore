// 'request_ids' view 
// doc.request is dictionary with request details
// see Insert._makeClipboardDoc how value are
// defined, basically e.g. req['request_id'] = req[u'RequestName'] ...
function(doc) 
{
	emit(doc.request.request_id, doc._id); 
}