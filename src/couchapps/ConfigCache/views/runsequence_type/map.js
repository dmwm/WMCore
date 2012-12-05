function(doc)
{
	if (doc.type)
	{
		if (doc.type == "runsequence")
		{
			emit(doc._id, {"owner_id": doc.owner_id});
		}
	}
}