// byhostnamebysource view - possible to query by both HostName and Source values
// http://localhost:5984/alertsdb/_design/Alerts/_view/test?
// startkey=[%22vocms888.cern.ch%22,%22CouchCPUPoller%22]&endkey=[%22vocms888.cern.ch%22,%22CouchCPUPoller%22]

function(doc)
{
	emit([doc.HostName, doc.Source], doc);
};