// byhostname view - returns all alerts for particular requested HostName
// curl $COUCHURL/alertsdb/_design/Alerts/_view/byhostname?key=\"vocms888.cern.ch\"

function(doc)
{	
	emit(doc.HostName, doc);
};