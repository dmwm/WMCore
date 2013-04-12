// all view - returns all requests sorted by RequestName
// (probably not very useful, perhaps not even used anywhere)

function(doc) 
{
	emit(doc.RequestName, doc);
}