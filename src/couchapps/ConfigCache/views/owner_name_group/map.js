function(doc) 
{
	if (doc.owner)
	{
		emit([doc.owner.group, doc.owner.user], doc._id);
	}
}