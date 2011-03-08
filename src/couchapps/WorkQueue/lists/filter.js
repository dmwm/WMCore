function(head, req) {

	// Return elements, in json list, that match given criteria
	// Optionally, return id of matching elements only

	if (!req.query.filter) {
		send('"Filter parameters required"');
		return;
	}
	var query = JSON.parse(req.query.filter);
	var idOnly = JSON.parse(req.query.idOnly);

	send("[");
	var first = true;
	while (row = getRow()) {
		ele = row["doc"]["WMCore.WorkQueue.DataStructs.WorkQueueElement.WorkQueueElement"];
		var matched = true;
		for (key in query) {

			if (key === 'since' && query[key] > row.timestamp) {
				matched = false;
				break;
			}
	
			if (key === 'after' && query[key] < row.timestamp) {
				matched = false;
				break;
			}
			
			if (key === 'reqMgrUpdateNeeded' && query[key] === 'true') {
				if (row.updatetime > row.reqmgrupdatetime) {
					matched = false;
					break;
				}
			}

			// for any other key just do a straight comparison
			if (ele[key] && query[key] != ele[key]) {
				matched = false;
				break
			}
		}

		if (!matched) {
			continue // element doesn't fit requirements
		}

		if (first != true) {
  		  send(",")
  	  	}

		// Send element or id depending on what was asked
		if (idOnly) {
			send(toJSON(row['id']));
		} else {
			send(toJSON(row["doc"]));  // need whole document, id etc...
		}
		first = false; // from now on prepend "," to output
	}

	send("]");	
}