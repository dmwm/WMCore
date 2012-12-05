function(doc) 
{
	if (doc.type)
	{
		if (doc.type == "config")
		{
			var label = "";
			if (doc.description)
			{
				label = doc.description.config_label;
			}
			emit([doc.owner.group, doc.owner.user], {"config_doc": doc._id, "config_label": label}) 
		}
	}
}