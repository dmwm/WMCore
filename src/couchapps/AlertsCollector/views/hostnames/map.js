// hostnames view - returns all HostNames from the alerts

function(doc)
{	
	emit(doc.HostName, null);
};