// bydate view - returns all requests sorted by RequestDate
// curl $COUCHURL/reqmgr_workload_cache/_design/ReqMgr/_view/bydate?descending=true&limit=1

function(doc) {
    if (doc.RequestDate && (doc.RequestDate.length >= 4)){
        var dateArray = doc.RequestDate;
        // get year, month, day, hour
        emit([parseInt(dateArray[0]), parseInt(dateArray[1]), parseInt(dateArray[2]), parseInt(dateArray[3])],
		                   {"RequestDate": doc.RequestDate,
		                    "RequestName": doc.RequestName,
		                    "RequestType": doc.RequestType,
		                    "RequestStatus": doc.RequestStatus});
	}
}