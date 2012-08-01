// all view - returns all alerts sorted by Timestamp

function(doc)
{
	emit(doc.Timestamp, doc);
};