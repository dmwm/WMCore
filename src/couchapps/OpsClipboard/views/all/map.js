// 'all' view
// returns all requests docs by ordered state
function(doc)
{
	emit(doc['state'], doc._id);
}