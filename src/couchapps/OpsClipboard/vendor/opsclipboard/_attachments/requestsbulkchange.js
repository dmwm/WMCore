// requestsbulkchange.js
//
// lists all requests and make possible to submit a state change
// at multiple requests at once


var bulkChange = 
{
    mainUrl: null, // full URL to the couchapp
    
    setUp: function()
    {
		utils.checkAndSetConsole();
		bulkChange.mainUrl = utils.getMainUrl(document.location.href);
    }, // setUp()
    
    
    // trawl the table contents, find all the checked entries and the new states
    // and fire off the changestate requests
    submitBulkChange: function()
    {
        console.log("submitBulkChange");
        var rowsToSubmit = bulkChange.getCheckedRows();
        for (r in rowsToSubmit)
        {
            var docId = rowsToSubmit[r].documentId;
            var url = bulkChange.mainUrl + "_update/changestate/" + docId;
            var data = {"newState": rowsToSubmit[r].newState};
            var options = {"method": "POST", "reloadPage": true};
        	utils.makeHttpRequest(url, null, data, options);
        }
    }, // submitBulkChange()
    
    
    // find which table rows have the checked box checked
    getCheckedRows: function()
    {
    	console.log("getCheckedRows");
        var results = []
        var table = document.getElementById("bulkrequesttableid");
        for (var index=0; index<table.rows.length; index++)
        {
        	// need to skip the very first row - no data rows, it's a header
        	if (index === 0) { continue; }
            var row = table.rows[index];
            // cells / columns are organised in a row as follows:
            // 0 - request id, 1 - current state, 2 - new state, 3 - check box
            var listbox = row.cells[2].firstChild;
            var checkbox = row.cells[3].firstChild;            
            if (checkbox.checked)
            {
                var result = {"requestId": row.requestId,
                		      "documentId": row.documentId,
                		      "newState": null};
                if (listbox.selectedIndex >= 0)
                {
                    var selected = listbox.options[listbox.selectedIndex];
                    result.newState = selected.value;
                }
                if (result.newState != null)
                {
                    results.push(result);
                } 
            }
        }
        console.log(results);
        return results;     
    }, // getCheckedRows()
    
    
    // build the bulk change table and submit button
    bulkChange : function(elemId)
    {
        console.log("bulkChange");
        var table = document.createElement("table");
        table.id = "bulkrequesttableid";
        // entire table style
        table.style.border = "2px solid black";
        table.style.textAlign = "center";
        table.cellPadding = "5px";
        table.rules = "cols"; // "all";
        var header = table.createTHead();
        // 0 - request id, 1 - current state, 2 - new state, 3 - check box
        var hRow = header.insertRow(0);
        hRow.style.fontWeight = "bold";
        hRow.style.backgroundColor = "#DDDDDD";
        hRow.insertCell(0).innerHTML = "Request ID";
        hRow.insertCell(1).innerHTML = "Current State";
        hRow.insertCell(2).innerHTML = "New State";
        hRow.insertCell(3).innerHTML = "Change State"; 
        document.getElementById(elemId).appendChild(table);
        // create some space between the table and button
        document.getElementById(elemId).appendChild(document.createElement("br"));
        var button = document.createElement("input");
        button.type = "button";
        button.value = "Submit Changes";
        button.onclick = bulkChange.submitBulkChange;
        document.getElementById(elemId).appendChild(button);
      
        utils.addPageLink(bulkChange.mainUrl + "index.html", "Main Page");
    }, // bulkChange()
    
    
    addTableRow: function(reqId, docId, state, rowColor)
    {
        var possibleStates = requestStatus.statusList[state];
        var table = document.getElementById("bulkrequesttableid");
        // 0 - request id, 1 - current state, 2 - new state, 3 - check box
        console.log("adding:" + reqId + "  " + state + "  " + docId);
    	var row = table.insertRow(-1);
    	row.style.backgroundColor = rowColor;
    	row.insertCell(0).innerHTML = reqId;
    	row.insertCell(1).innerHTML = state;  
        var menu = document.createElement("select");
        for (x in possibleStates)
        {
            menu.options[x] = new Option(possibleStates[x], possibleStates[x], true, false);
        }
    	row.insertCell(2).appendChild(menu);
        var checkbox = document.createElement("input");
        checkbox.type = "checkbox";
        if (possibleStates.length === 0)
        {
            checkbox.disabled = true;
        }
        row.insertCell(3).appendChild(checkbox);
        row.requestId = reqId;
        row.documentId = docId;
    }, // addTableRow()
    
    
    processData: function(data)
    {
		for (i in data.rows)
		{
			var reqId = data.rows[i].key;
			var docId = data.rows[i].value['doc_id'];
			var state = data.rows[i].value['state'];
            // alternate colours in table rows
            var rowColor = i % 2 === 0 ? "#FAFAFA" : "#E3E3E3";      						
			bulkChange.addTableRow(reqId, docId, state, rowColor);
		}
    }, // processData()
    
    
    // load the couch view and populate the table.
    // each table row is tagged with the request id that can be used
    // to look up and modify the table when bulk changes are committed
    bulkChangeUpdate: function()
    {
        var url = bulkChange.mainUrl + "_view/request";
        var options = {"method": "GET", "reloadPage": false};
        utils.makeHttpRequest(url, bulkChange.processData, null, options); 
    } // bulkChangeUpdate()
    
} // bulkChange