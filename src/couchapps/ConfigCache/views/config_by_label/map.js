function(doc) 
{
	if (!doc.description) 
	{
		return;
	}
	else if (!doc.description.config_label) 
	{
		return;
	}
	emit(doc.description.config_label, doc._id);
}