//requestsexpunge.js
//
// lists all requests in "ReadyToRelease", "ReadyToReject" states and offer
// to expunge them from OpsClipboard and set their next state in RequestManager


var requestsExpunge = 
{
    couchdb : null,
    mainpage: null,
    reqMgrUrl: "http://localhost:8687/reqmgr/reqMgr/",
    
    setUp: function()
    {
		utils.checkAndSetConsole();
        var dbname = document.location.href.split('/')[3];
        console.log("couchdb ref set: " + dbname)
        this.couchdb = $.couch.db(dbname);
        this.mainpage = this.couchdb.uri + "_design/OpsClipboard/index.html";
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
            	// not clear how to decide if 'aborted' or 'failed' (or some other) should be applied
            	// listbox in the table?
            	state = 'failed';
            }
            var reqId = rowsToSubmit[r].requestId;
            var docRev = rowsToSubmit[r].documentRev;
            
            // make ReqMgr REST call - advance to the next state
            var reqMgrUrl = requestsExpunge.reqMgrUrl + "request/" + reqId;
            console.log("ReqMgr call: " + reqMgrUrl);
            // TODO
            // to double-check, seeing from Firefox: Unsupported verb: OPTIONS
            $.ajax({url: reqMgrUrl,
            	    type: "PUT",
            	    data:  "status=" + state,
                    success : function(resp) { console.log(resp); },
                    error: function(resp) { console.log("Call to ReqMgr failed, reason: " + resp); }
                   });
            
            // remove from OpsClipboard
            // need to put revision to avoid {"error":"conflict","reason":"Document update conflict."}
            var couchUri = requestsExpunge.couchdb.uri + docId + "?rev=" + docRev;
            console.log("removing doc from couch (uri): " + couchUri);            
            $.ajax({url: couchUri,
            	    type: "DELETE",
            	    success : function(resp) { console.log(resp); }
                   });
            
            // another solution experimented with - gives syntax error
            //requestsExpunge.couchdb.openDoc(docId, 
            //{
            //	success: function(doc) 
            //	{
            //		requestsExpunge.couchdb.removeDoc(doc,
            //		{
            //			success: function()
            //			{
            //				console.log("deleted");
            //           },
            //            error: function()
            //           {
            //            	alert("Could not delete document(s) from CouchDB.");
            //            }
            //        })
            //    }
            //});            
        }
        window.location.reload();
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
        button.onclick = this.submitExpunge;
        document.getElementById(elemId).appendChild(button);
      
        utils.addPageLink(requestsExpunge.mainpage, "Main Page");
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
        var clipLink = "<a href=\"" + requestsExpunge.couchdb.uri ;
        clipLink += "_design/OpsClipboard/_show/request/" + docId + "\">" + reqId + "</a>";
        row.insertCell(2).innerHTML = clipLink;        
        var checkbox = document.createElement("input");
        checkbox.type = "checkbox";
        row.insertCell(3).appendChild(checkbox);
        row.requestId = reqId;
        row.documentId = docId;
        row.state = state;
        row.documentRev = docRev;
    }, // addTableRow()
    
    
    // load the couch view and populate the table.
    requestsExpungeUpdate: function()
    {
    	this.couchdb.view("OpsClipboard/expunge",
    			{
                	success : function(data)
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
                	}
    			});
    } // requestsExpungeUpdate()
    
} // requestsExpunge