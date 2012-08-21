// bysource view - returns all alerts for particular requested Source
// curl $COUCHURL/alertsdb/_design/Alerts/_view/bysource?key=\"ErrorHandlerPoller\"

function(doc)
{	
	emit(doc.Source, doc);
};