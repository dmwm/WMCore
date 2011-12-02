// this is to reduce duplicates in campaign_ids names

// TODO
// probably clumsy way of getting rid of duplicates in CouchDB ... 
function(keys, values) 
{
	function inArray(array, value) 
	{
	    for (var index in array) 
	    {
	        if (array[index] == value)
	        {
	        	return true;
	        }
	    }
	    return false;
	}

	var result = [];
	// the keys, as returned by the map.js is in the form of two-items lists:
	// [key, id] (key being what was returned by the map function) 
	// e.g. ['campaign_2', '591e734d10e6a46ee50e3357ec05052c'], hence keys[i][0] 
	for (var index in keys)
	{
		if (! inArray(result, keys[index][0]))
		{
			result.push(keys[index][0]);
		}
	}
	return result.sort();	
}