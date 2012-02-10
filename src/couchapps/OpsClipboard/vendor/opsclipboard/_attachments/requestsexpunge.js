//requestsexpunge.js
//
// lists all requests in "ReadyToRelease", "ReadyToReject" states and offer
// to expunge them from OpsClipboard and set their next state in RequestManager


var requestsExpunge = 
{
	mainUrl: null, // full URL to the couchapp
	// ReqMgr REST API URL (rather than hard-code one)
    reqMgrUrl: null,
    
    setUp: function()
    {
		utils.checkAndSetConsole();
		requestsExpunge.mainUrl = utils.getMainUrl(document.location.href);
		// document.location.origin is unfortunately undefined ... (that would have had everything)
		requestsExpunge.reqMgrUrl = document.location.protocol + "//" + 
		                            document.location.host + "/reqmgr/reqMgr/";
    }, // setUp()
    
    
    // submit an expunge request: remove requests from OpsClipboard
    // and make REST call to ReqMgr to set appropriate state there:
    // 'assigned' for ReadyToRelease, 'failed/aborted' for ReadyToReject    
    // trawl the table contents, find all the checked entries
    submitExpunge: function()
    {
        console.log("submitExpunge");
        var rowsToSubmit = requestsExpunge.getCheckedRows();
        for (r in rowsToSubmit)
        {
            var docId = rowsToSubmit[r].documentId;
            // new ReqMgr state
            var state = null;
            if (rowsToSubmit[r].state == "ReadyToRelease")
            {
            	state = "assigned";
            }
            if (rowsToSubmit[r].state == "ReadyToReject")
            {
            	// TODO
            	// not clear how to decide if 'aborted' or 'failed' (or some other) 
            	// state should be set at ReqMgr ; listbox in the table?
            	state = 'failed';
            }
            var reqId = rowsToSubmit[r].requestId;
            var docRev = rowsToSubmit[r].documentRev;
            
            // make ReqMgr REST call - advance to the next state
            // how to make REST call from JavaScript:
            // http://developer.yahoo.com/yui/connection/
            // http://api.jquery.com/jQuery.ajax/ (similar to down for couchdb, but PUT method)
            // getting "Unsupported verb: OPTIONS" - same origin policy violation ...
            var url = requestsExpunge.reqMgrUrl + "request/" + reqId;
            var data = {"status": state};
            var options = {"method": "PUT", "reloadPage": false};                        
            utils.makeHttpRequest(url, null, data, options);
            // TODO
            // maybe should query ReqMgr to check status change
            // currently, this call on a request non-existent in ReqMgr throws
            // 500 error at ReqMgr but here propagates request.status = 0
            
            // remove from OpsClipboard
            // need to put revision to avoid {"error":"conflict","reason":"Document update conflict."}
        	// URL to get after the couch database name to be able to modify the document
            var url = requestsExpunge.mainUrl.split("_design")[0] + docId;
            var data = {"rev": docRev};
            var options = {"method": "DELETE", "reloadPage": true};
            utils.makeHttpRequest(url, null, data, options);
        }
    }, // submitExpunge()
    
    
    // find which table rows have the checked box checked
    getCheckedRows: function()
    {
    	console.log("getCheckedRows");
        var results = []
        var table = document.getElementById("requestsexpungetableid");
        for (var index=0; index<table.rows.length; index++)
        {
        	// need to skip the very first row - no data rows, it's a header
        	if (index === 0) { continue; }
            var row = table.rows[index];
            // cells / columns are organised in a row as follows:
            // 0 - state, 1 - updated, 2 - request id (OpsClipboard link) 3 - check box
            var checkbox = row.cells[3].firstChild;            
            if (checkbox.checked)
            {
                var result = {"requestId": row.requestId,
                		      "documentId": row.documentId,
                		      "documentRev": row.documentRev,
                		      "state": row.state};
                results.push(result);
            }
        }
        console.log(results);
        return results;     
    }, // getCheckedRows()
    
    
    // build the expunge table and submit button
    requestsExpunge : function(elemId)
    {
        console.log("requestsExpunge");
        var table = document.createElement("table");
        table.id = "requestsexpungetableid";
        // entire table style
        table.style.border = "2px solid black";
        table.style.textAlign = "center";
        table.cellPadding = "5px";
        table.rules = "cols"; // "all";
        var header = table.createTHead();
        // 0 - state, 1 - updated, 2 - request id (OpsClipboard link) 3 - check box
        var hRow = header.insertRow(0);
        hRow.style.fontWeight = "bold";
        hRow.style.backgroundColor = "#DDDDDD";
        hRow.insertCell(0).innerHTML = "Current State";
        hRow.insertCell(1).innerHTML = "Last Updated";
        hRow.insertCell(2).innerHTML = "Request ID";
        hRow.insertCell(3).innerHTML = "Expunge"; 
        document.getElementById(elemId).appendChild(table);
        // create some space between the table and button
        document.getElementById(elemId).appendChild(document.createElement("br"));
        var button = document.createElement("input");
        button.type = "button";
        button.value = "Expunge";
        button.onclick = requestsExpunge.submitExpunge;
        document.getElementById(elemId).appendChild(button);
      
        utils.addPageLink(requestsExpunge.mainUrl + "index.html", "Main Page");
    }, // requestsExpunge()
    
        	                    
    addTableRow: function(reqId, docId, docRev, state, lastUpdated, rowColor)
    {
        var table = document.getElementById("requestsexpungetableid");
    	// 0 - state, 1 - updated, 2 - request id (OpsClipboard link) 3 - check box        
        console.log("adding:" + reqId + "  " + state + "  " + docId, + "  " + lastUpdated);
    	var row = table.insertRow(-1);
    	row.style.backgroundColor = rowColor;
    	row.insertCell(0).innerHTML = state;
    	row.insertCell(1).innerHTML = new Date(parseInt(lastUpdated)).toLocaleString();
        var clipLink = "<a href=\"" + requestsExpunge.mainUrl;
        clipLink += "_show/request/" + docId + "\">" + reqId + "</a>";
        row.insertCell(2).innerHTML = clipLink;        
        var checkbox = document.createElement("input");
        checkbox.type = "checkbox";
        row.insertCell(3).appendChild(checkbox);
        row.requestId = reqId;
        row.documentId = docId;
        row.state = state;
        row.documentRev = docRev;
    }, // addTableRow()
    
    
    processData: function(data)
    {
		for (i in data.rows)
		{
			var state = data.rows[i].key;
			var updated = data.rows[i].value['updated'];
			var reqId = data.rows[i].value['request_id'];
			var docId = data.rows[i].value['doc_id'];
			var docRev = data.rows[i].value['rev'];
            // alternate colours in table rows
            var rowColor = i % 2 === 0 ? "#FAFAFA" : "#E3E3E3";    	                    
            requestsExpunge.addTableRow(reqId, docId, docRev, state, updated, rowColor);
		}
    }, // processData()
    

    // load the couch view and populate the table.
    requestsExpungeUpdate: function()
    {
        var url = requestsExpunge.mainUrl + "_view/expunge";
        var options = {"method": "GET", "reloadPage": false};
        utils.makeHttpRequest(url, requestsExpunge.processData, null, options);
    } // requestsExpungeUpdate()
    
} // requestsExpunge