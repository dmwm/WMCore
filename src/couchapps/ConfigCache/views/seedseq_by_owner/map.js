function(doc)
{
	if (doc.type)
	{
		if (doc.type == "seedsequence")
		{
			emit([doc.owner_id], {"seedseq_doc": doc._id, "seedseq_label": doc.seedseq_label});
		}
	}
}