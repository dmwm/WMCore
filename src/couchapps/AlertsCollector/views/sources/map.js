// sources view - returns all Source types for all alerts

function(doc)
{	
	emit(doc.Source, doc);
};