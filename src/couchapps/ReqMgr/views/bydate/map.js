// bydate view - returns all requests sorted by RequestDate
// curl $COUCHURL/reqmgr_workload_cache/_design/ReqMgr/_view/bydate?descending=true&limit=1

function(doc) 
{	
	if (doc.RequestDate && (doc.RequestDate.length > 0)){
      var dateArray = doc.RequestDate;
      // get year, month, day, min
      emit([dateArray[0], dateArray[1], dateArray[2], dateArray[3]],
		                   {"RequestDate": doc.RequestDate,
		                   "RequestName": doc.RequestName,
		                   "RequestType": doc.RequestType,
		                   "RequestStatus": doc.RequestStatus});
	}
}