function(doc)
{
	if (doc.type)
	{
		if (doc.type == "owner")
		{
			emit([doc.owner.group], [doc.owner.user, doc._id]);
		}
	}
}