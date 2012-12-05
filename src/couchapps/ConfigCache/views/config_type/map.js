function(doc) 
{
	if (doc.type)
	{
		if (doc.type == "config")
		{
			emit(doc._id, doc.owner_id);
		}
	}	
}