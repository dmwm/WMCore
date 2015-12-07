function(doc) {
	if (doc.Teams) {
		emit([doc.Teams[0], doc.RequestStatus], null);
	}
}
