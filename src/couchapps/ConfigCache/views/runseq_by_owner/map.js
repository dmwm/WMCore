function(doc)
{
	if (doc.type)
	{
		if (doc.type == "runsequence")
		{
			emit([doc.owner_id], {"runseq_doc": doc._id, "runseq_label": doc.runseq_label});
		}
	}
}