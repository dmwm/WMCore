function(doc)
{ 
	if (doc.Timestamp != null)
	{
		emit(doc.Timestamp, doc);
	}
};