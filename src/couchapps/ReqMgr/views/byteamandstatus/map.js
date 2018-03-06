function(doc) {
	if (doc.Team) {
		emit([doc.Team, doc.RequestStatus], null);
	}
}
