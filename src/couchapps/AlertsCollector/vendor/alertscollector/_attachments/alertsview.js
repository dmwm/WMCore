/* alertsview.js
 * 
 * presents a tabled list of alerts
 * 
 */


var alertsView = 
{
    mainUrl: null, // full URL to the couchapp
    limit: null, // limits max. number of alerts displayed on the page by default
    data: null, // data as loaded from the server
    // holds the information about the index (to the alertsView.data table) of
    // the alert that is next to be displayed if demanded (if only most recent alerts are displayed)
    nextAlertIndexToDisplay: null,
    closedCellInnerHTML: "&nbsp;<img src=\"img/details_closed.gif\"</img>&nbsp;",
    openCellInnerHTML: "&nbsp;<img src=\"img/details_open.gif\"</img>",
    
    
    /* 
     * main page building & display
     * build a table in the supplied HTML element
     * mainElemId is parent elementId
     */
    define: function(input)
    {
		utils.setUp();
		alertsView.mainUrl = utils.getMainUrl(document.location.href);
		alertsView.limit = input.limit;
		
		var mainElemId = input.contentDivId;
		var mainElem = document.getElementById(mainElemId); // this is the main content div
		console.log("alertsView.define() - building the table");
		
        // do page title
        var pageTitle = document.createElement("div");
        pageTitle.innerHTML = "Displayed Alerts: ";
        pageTitle.id = "pagetitle";
        mainElem.appendChild(pageTitle);
        
        // create by HostName selection filter
        var input = {"title": "Filter by HostName",
        		     "selectorId": "hostnameselectid",
        		     "handler": alertsView.bySelectionUpdate};        		  
        var table = alertsView.setUpSelectionFilter(input);
        mainElem.appendChild(table);
        
        mainElem.appendChild(document.createElement("br"));

        // create by Source selection filter
        var input = {"title": "Filter by Source",
		   		     "selectorId": "sourceselectid",
		   		     "handler": alertsView.bySelectionUpdate};        	
        var table = alertsView.setUpSelectionFilter(input);
        mainElem.appendChild(table);
       
        mainElem.appendChild(document.createElement("br"));
        
        // set up the main table for alerts
        var table = alertsView.setUpAlertsTable();
        // document.getElementById(mainElemId).appendChild(table);
        
        var alertsDiv = document.createElement("div");
        alertsDiv.className = "alertscontent";
        alertsDiv.id = "alertscontentid";
        alertsDiv.appendChild(table);
        
        mainElem.appendChild(alertsDiv);
        
        mainElem.appendChild(document.createElement("br"));
        
        // create a div under the table: "display more alerts" | "no more alerts"
        var loadMoreBottomDiv = mainElem.appendChild(document.createElement("div"));
        loadMoreBottomDiv.id = "loadmorebottomdivid";
        loadMoreBottomDiv.className = "alertscontent loadmorebottom";
        loadMoreBottomDiv.innerHTML = "&nbsp;";
        // having <a> javascript:func defined plus this line below causes function called twice
        // when having <a>, this call not necessary
        // loadMoreBottomDiv.onclick = alertsView.loadMoreAlerts; 
    
        mainElem.appendChild(document.createElement("br"));
        mainElem.appendChild(document.createElement("br"));
                
    }, // define()
    

    /*
     * The method is called onclick the 'See more' button (div). And that is
     * only displayed when there is limit (only recent alerts are displayed)
     * and there really is something to display (the div is hidden otherwise).
     */
    loadMoreAlerts: function()
    {
    	var loadMoreBottomDiv = document.getElementById("loadmorebottomdivid");
    	console.log("loadMoreAlerts() called ... " +
    			    "next index to display: " + alertsView.nextAlertIndexToDisplay);
    	loadMoreBottomDiv.innerHTML = "&nbsp;";
    	// the loading sign is not really necessary in this GUI configuration, was
    	// mainly an exercise ...
    	// display loading sign on the div
    	loadMoreBottomDiv.className = "alertscontent loadmorebottom loadmorebottomloading";
    	
    	var tableElem = document.getElementById("alertsviewtable");
    	var startIndex = alertsView.nextAlertIndexToDisplay;
    	var endIndex = startIndex + config.limitedViewNumOfMoreAlerts;
    	// add alerts into the already displayed tableElem
    	alertsView.nextAlertIndexToDisplay = alertsView.displayAlerts(tableElem, startIndex, endIndex);
    	
    	// turn the loading sign off on the div
    	loadMoreBottomDiv.className = "alertscontent loadmorebottom";
    	if (alertsView.nextAlertIndexToDisplay < alertsView.data.rows.length)
    	{
    		loadMoreBottomDiv.style.visibility = "visible";
    		loadMoreBottomDiv.innerHTML = "<a href=\"javascript:alertsView.loadMoreAlerts()\">See more »</a>";    		
    	}
    	else
    	{
    		loadMoreBottomDiv.style.visibility = "hidden";
    	}

    }, // loadMoreAlerts()
    
    
    /*
     * create a table which hosts filter title and the selection
     * drop-down box itself. returns the table definition.
     */
    setUpSelectionFilter: function(input)
    {
        var table = document.createElement("table");
        var row = table.insertRow(-1);
        row.insertCell(0).innerHTML = input["title"];
        row.className = "boldtext";
        var selector = document.createElement("select");
        selector.id = input["selectorId"];
    	var option = document.createElement("option");
    	option.text = "unspecified";
    	selector.add(option, null);
    	selector.onchange = function() { input["handler"](); };
        row.insertCell(1).appendChild(selector);
        return table;
    }, // setUpHostNameSelectionTable()
    	
    
    /*
     * create a table for displaying alerts, this function
     * defines table header
     */
    setUpAlertsTable: function()
    {
	    var table = document.createElement("table");
	    table.id = "alertsviewtable";
	    table.cellPadding = "5px"; // doesn't work when applied in style
	    // first row is header
	    // 0 - Timestamp, 1 - HostName, 2 - AgentName, 3 - Component, 4 - Source 
	    var hRow = table.insertRow(0);
	    hRow.className = "boldtextandbackground";
	    hRow.insertCell(0).innerHTML = "&nbsp;"; // open|close alert details toggle
	    hRow.insertCell(1).innerHTML = "Timestamp";
	    hRow.insertCell(2).innerHTML = "Host Name";
	    hRow.insertCell(3).innerHTML = "Agent Name";
	    hRow.insertCell(4).innerHTML = "Component";
	    hRow.insertCell(5).innerHTML = "Source";
	    return table;
    }, // setUpAlertsTable()
    

    /* called on-change by HostName select drop-down menu to narrow the view
     * called on-change by Source select drop-down menu to narrow the view
     * CouchDB queries made on every change here: 
     * 		selection only by HostName corresponds to byhostname view
     * 		selection only by Source corresponds to by source view
     * 		selection by both HostName and Source together corresponds
     * 			to byhostnamebysource view
     * Another approach would be just to filter already loaded data without
     * querying CouchDB.
     * 
     * content of the drop-down menus is not modified here
     */
    bySelectionUpdate: function()
    {
    	function getListboxSelection(selectorId)
    	{
	        var selectOptions = document.getElementById(selectorId).options;
	    	var selectItem = selectOptions[selectOptions.selectedIndex].text;
	    	console.log("selection: " + selectItem);
	    	var selected = null;
	    	if (selectItem !== "unspecified")
	    	{
	    		var selected = selectItem.split(' ')[0];
	    	}
	    	return selected;
    	}
    
    	console.log("bySelectionUpdate()");
    	// check HostName selection
    	var host = getListboxSelection("hostnameselectid");
    	// check Source selection
    	var source = getListboxSelection("sourceselectid");
    	var url = alertsView.mainUrl;
    	var data = null;
    	if (host && !source)
    	{
    		data = {"startkey": host, "endkey": host, "descending": true};
    		url += "_view/byhostname";
    	}
    	else if (!host && source)
    	{
    		data = {"startkey": source, "endkey": source, "descending": true};
    		url += "_view/bysource";
    	}
    	else if (host && source)
    	{
    	
    		data = {"startkey": [host, source], "endkey": [host, source], "descending": true};
    		url += "_view/byhostnamebysource";
    	}
    	else
    	{
    		url += "_view/all";
    	}
    	
        var options = {"method": "GET", "reloadPage": false};    	

        // this section would recreate the entire table based on the selection
        // above. the table is created from scratch which is not optimal in case
        // of many DOM manipulations for they are expensive.
        // table deleteRow(index or -1) is superslow, can't be used
                
    	var alertsDiv = document.getElementById("alertscontentid");
    	var table = document.getElementById("alertsviewtable");
    	alertsDiv.removeChild(table);
    	var newTable = alertsView.setUpAlertsTable();
    	alertsDiv.appendChild(newTable);
    	
        utils.makeHttpRequest(url, alertsView.processAlertsData, data, options);
        
	    // set the loading sign on, may check if (document.readyState == "loading")
	    document.getElementById("loadingshadowdivid").className = "displayed";
    }, // bySelectionUpdate()
        
    
    addTableRow: function(tableRow, time, host, agent, component, docId, source)
    {
    	// multiplied by 1000 so that the argument is in milliseconds, not seconds
    	// (Python vs JavaScript discrepancy)
    	var dateTime = new Date(parseInt(time * 1000)).toLocaleString();
    	// link to the couch document representing the alert
    	var url = alertsView.mainUrl.split("_design")[0] + docId;
        var couchLink = "<a href=\"" + url + "\" target=_blank>" + source + "</a>";
    	// 0 - Timestamp, 1 - HostName, 2 - AgentName, 3 - Component, 4 - Source
    	// approx. 5x faster to process than row.insertCell(0).innerHTML = dateTime; ...
    	// however doing innerHTML <tr> on the entire table with 14 alerts never finishes ...
        tableRow.innerHTML = "<td>" + alertsView.closedCellInnerHTML + "</td>" + 
        	                 "<td>" + dateTime + "</td>" +
    	   	                 "<td>" + host + "</td>" +
    	   	                 "<td>" + agent + "</td>" +
    	   	                 "<td>" + component + "</td>" +
    	   	                 "<td>" + couchLink + "</td>";
        tableRow.onmouseover = alertsView.alertTableMouseOverHandler;
        tableRow.onmouseout = alertsView.alertTableMouseOutHandler;
        // clickable open|closed toggle entire row (could also be on 
        // first cell tableRow.cells[0] only)
        tableRow.onclick = alertsView.alertDetailsToggle;
        // detailsToggle - arbitrary flag
        tableRow.detailsToggle = "closed";
    }, // addTableRow()
    
    
    alertTableMouseOverHandler: function()
    {
    	this.className = "onHover";
    	
    }, // alertTableMouseOverHandler()
    
    
    alertTableMouseOutHandler: function()
    {	
    	this.className = this.alertArrayId % 2 === 0 ? "even" : "odd";   
    	
    }, // alertTableMouseOutHandler()
    
    
    alertDetailsToggle: function()
    {
    	/* returns constructed div to display in the unfolded, spanned
    	 * table row with all Alert details.
    	 */
    	function getAlertDetails(alert)
    	{
    		var mainDiv = document.createElement("div");
    		mainDiv.className = "alertDetails";
    		var mainTable = document.createElement("table");
    		// table for base Alert items (baseKeys)
    		var baseTable = document.createElement("table");
    		baseTable.className = "alertDetails";
    		// keys/values already shown:  Timestamp, Host Name, Agent Name, Component, Source
    		var baseKeys = ["Workload", "TeamName", "Level", "Source", "Type", "Contact"];
    		for (i in baseKeys)
    		{
    			var row = baseTable.insertRow(-1);
    			var cell = row.insertCell(-1);
    			cell.className = "alertDetails";
    			cell.innerHTML = baseKeys[i] + ":&nbsp;";
    			cell = row.insertCell(-1);
    			cell.className = "alertDetailsBold";
    			cell.innerHTML = alert[baseKeys[i]];
    		}
    		
    		var mainTableRow = mainTable.insertRow(-1);
    		mainTableRow.className = "alertDetails";
    		// fist cell with base Alert description (baseTable)
    		var mainTableCell = mainTableRow.insertCell(-1);
    		mainTableCell.appendChild(baseTable);
    		
    		// second cell with Alert details 
    		mainTableCell = mainTableRow.insertCell(-1);
    		var tableDetails = document.createElement("table");
    		tableDetails.className = "alertDetails";
    		var detailsRow = tableDetails.insertRow(-1);
    		detailsRow.className = "alertDetails";
    		detailsRow.innerHTML = "<td colspan=\"2\" class=\"alertDetails\">Details:</td>";
    		var alertDetails = alert["Details"];
        	for (key in alertDetails) 
        	{        		
        	    if (alertDetails.hasOwnProperty(key)) 
        	    {
        	    	var row = tableDetails.insertRow(-1);
        	    	var cell = row.insertCell(-1);
        	    	cell.className = "alertDetails";
        	    	cell.innerHTML = "&nbsp;&nbsp;" + key + ":&nbsp;";
        	    	cell = row.insertCell(-1);
        	    	cell.className = "alertDetailsBold";
        	    	cell.innerHTML = alertDetails[key];
        	    }
        	}    		
        	mainTableCell.appendChild(tableDetails);
  
    		mainDiv.appendChild(mainTable);
    		return mainDiv;
    	}

    	
    	// issues with index counting into table when inserting 
    	// and deleting rows dynamically (.insertRow() / .deleteRow())
    	// this function counts open rows above the currently processed row
    	function getNumOpenRowsAbove(alertArrayId, tableElem)
    	{
    		var c = 0;
    		for (i in tableElem.rows)
    		{
    			// have this line first so that not to count its own open row
    			if (tableElem.rows[i].alertArrayId === alertArrayId) { break; }
    			if (tableElem.rows[i].detailsToggle === "open") { c++; }
    		}
    		return c;
    	}
    	
    	var alert = alertsView.data.rows[this.alertArrayId].value;
    	console.log("alertDetailsToggle() alertArrayId: " + this.alertArrayId +
    			   " CouchDB id: " + alert["_id"]);
    	var tableElem = document.getElementById("alertsviewtable");
    	if (this.detailsToggle === "closed")
    	{
    		// toggle open
    		// alertArrayId is used also as index for table rows
    		// +1: table header +1: want to add the details row below the its parent
    		// + count +1 for each open row above this one (open details is extra row)
    		var counter = getNumOpenRowsAbove(this.alertArrayId, tableElem);
    		var tableIndex = this.alertArrayId + 2 + counter;
    		console.log("table index: " + tableIndex);
    		var row = tableElem.insertRow(tableIndex);
    		// from the table definition, there are 6 columns
    		row.innerHTML = "<td colspan=\"6\"></td>";
    		row.cells[0].appendChild(getAlertDetails(alert));
    		this.cells[0].innerHTML = alertsView.openCellInnerHTML;
    		this.detailsToggle = "open";    		
    	}
    	else
    	{
    		// toggle close
    		// alertArrayId is used also as index for table rows
    		// + count +1 for each open row above this one (open details is extra row)
    		var counter = getNumOpenRowsAbove(this.alertArrayId, tableElem);
    		var tableIndex = this.alertArrayId + 2 + counter;
    		console.log("table index: " + tableIndex);
    		tableElem.deleteRow(tableIndex);
    		this.cells[0].innerHTML = alertsView.closedCellInnerHTML;
    		this.detailsToggle = "closed";    		
    	}    	
    }, // alertDetailsToggle()
    	
    
    /*
     * Based on startIndex and endIndex into internal alertsView.data table
     * it adds rows for corresponding alerts into the tableElem.
     */
    displayAlerts: function(tableElem, startIndex, endIndex)
    {
    	var totalNumAlerts = alertsView.data.rows.length;
    	var endIndex = (endIndex > totalNumAlerts) ? totalNumAlerts : endIndex;
	    var tableRow;
		// 0 - Timestamp, 1 - HostName, 2 - AgentName, 3 - Component, 4 - Source        
		for (var i=startIndex; i<endIndex; i++) 
		{
			var time = alertsView.data.rows[i].value["Timestamp"];
	        var host = alertsView.data.rows[i].value["HostName"];
	        var agent = alertsView.data.rows[i].value["AgentName"];
	        var component = alertsView.data.rows[i].value["Component"];
	        var source = alertsView.data.rows[i].value["Source"];
	        var docId = alertsView.data.rows[i].value["_id"];
	        var rowClass = i % 2 === 0 ? "even" : "odd";
	        tableRow = tableElem.insertRow(-1);
	        tableRow.className = rowClass;
	        tableRow.alertArrayId = i;
	        alertsView.addTableRow(tableRow, time, host, agent, component, docId, source);
		}
		// output number of returned alerts
		var pageTitle = document.getElementById("pagetitle");
		// indices start from 0, i index is not displayed (the preceding one is), 
		// but total number of displayed alerts is i (plus whatever was already
		// previously displayed), i is next index to display 
	    pageTitle.innerHTML = "Displayed Alerts: " + i + " of total " + totalNumAlerts;
		return i;
    }, // displayAlerts()

    
    /*
     * Called to process data returned by the view.
     * Function is specified as callback for http query and processes result.
     */
    processAlertsData: function(data)
    {
    	alertsView.data = data;
    	    	
    	/*
    	 * byhostname, byhostnamebysource, bysource views need to be sorted by
    	 * doc.Timestamp
    	 * since they don't have Timestamp as key like the all view
    	 * takes about additional 10ms on already sorted list from
    	 * the 'all' view, do always
    	*/
    	function sorter(a, b)
    	{
    		// sorts data according to Timestamp
    		return b.value["Timestamp"] - a.value["Timestamp"];
    	}
    	alertsView.data.rows.sort(sorter);
    	
    	var tableElem = document.getElementById("alertsviewtable");
    	console.log("processAlertsData() rows in input data: " + alertsView.data.rows.length + 
    			    " rows in current table: " + tableElem.rows.length);
    	var startTime = new Date().getTime();
    	
        // display only most recent data (limit - number of alerts)
        var limit = alertsView.limit ? alertsView.limit : alertsView.data.rows.length;
   
        alertsView.nextAlertIndexToDisplay = alertsView.displayAlerts(tableElem, 0, limit);
        
    	var endTime = new Date().getTime();
    	
    	// set the loading sign off
    	document.getElementById("loadingshadowdivid").className = "hidden";
    	utils.printTiming(startTime, endTime, "processAlertsData()");
    	
    	// check what to display at the bottom 'See more ' button
    	var loadMoreBottomDiv = document.getElementById("loadmorebottomdivid");
    	
    	if (alertsView.limit && alertsView.nextAlertIndexToDisplay < alertsView.data.rows.length)
    	{
    		loadMoreBottomDiv.style.visibility = "visible";
    		loadMoreBottomDiv.innerHTML = "<a href=\"javascript:alertsView.loadMoreAlerts()\">See more »</a>";
    	}
    	else
    	{
    		loadMoreBottomDiv.style.visibility = "hidden";
    	}    	
    }, // processAlertsData()
    
    
    /*
     * Get data for the selection filters and process response from server.
     */
    setSelectionFilter: function(input)
    {
    	var url = alertsView.mainUrl + "_view/" + input["viewName"];
    	var data = {"group": true};
    	var options = {"method": "GET", "reloadPage": false};
    	var selector = document.getElementById(input["selectorId"]);
    	utils.makeHttpRequest(url, function(data) 
    	{
    		for (i in data.rows) 
    		{
    	    	var selectItem = data.rows[i]["key"];
    	    	var numAlertsPerItem = data.rows[i]["value"];
    	    	var option = document.createElement("option");
    	    	option.text = selectItem + " (" + numAlertsPerItem + " total alerts)";
    	    	selector.add(option, null);
    	    }
    	}, data, options);	
    }, // setSelectionFilter()
    
    
    /*
     * Main function called upon page update.
     * Queries 'all' CouchDB view and populates the HTML page.
     */
	update: function()
	{
    	// view returns list of alerts sorted by Timestamp
		var url = alertsView.mainUrl + "_view/all";
		var data = {"descending": true};
	    var options = {"method": "GET", "reloadPage": false};	    
	    utils.makeHttpRequest(url, alertsView.processAlertsData, data, options);
	    var input = {"viewName": "hostnames", "selectorId": "hostnameselectid"};
	    alertsView.setSelectionFilter(input);
	    var input = {"viewName": "sources", "selectorId": "sourceselectid"};
	    alertsView.setSelectionFilter(input);
	    
	    // set the loading sign on, may check if (document.readyState == "loading")
	    document.getElementById("loadingshadowdivid").className = "displayed";
	} // update()

		
} // alertsView