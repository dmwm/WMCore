function(keys, values, rereduce)
{
	var unique_labels = {};
	values.forEach(function(label) 
			{
				if (!unique_labels[label]) 
				{
					unique_labels[label] = true;
				}
			});
	return unique_labels;
}