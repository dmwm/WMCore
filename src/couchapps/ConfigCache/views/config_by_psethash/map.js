function(doc) 
{
	if (doc.pset_hash)
	{
		emit(doc.pset_hash,{'_id': doc._id, '_rev': doc._rev});
	}
}
