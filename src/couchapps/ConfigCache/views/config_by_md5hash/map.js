function(doc)
{
	if (doc.md5_hash)
	{
		emit(doc.md5_hash,{'_id': doc._id, '_rev': doc._rev});
	}
}