/* requestsview.js
 * 
 * presents a tabled list of ReqMgr requests
 * 
 * TODO:
 * - search ajax box like in SiteDB (no selection list-box filters)
 * - instead of table try multi-column CSS like SiteDB
 * - @MOUNT@ as used in index.html doesn't work here 
 * 
 * 
 */

var requestsView = 
{
	mainAppUrl: "/reqmgr2",
    closedCellInnerHTML: "&nbsp;<img src=\"/reqmgr2/static/?html/ReqMgr/img/details_closed.gif\"</img>&nbsp;",
    openCellInnerHTML: "&nbsp;<img src=\"/reqmgr2/static/?html/ReqMgr/img/details_open.gif\"</img>",				
    
    /* 
     * main page building & display
     * build a table in the supplied HTML element
     * mainElemId is parent elementId
     */
    define: function(input)
    {
		utils.setUp();
		
		var mainElemId = input.contentDivId;
		var mainElem = document.getElementById(mainElemId); // this is the main content div
		
    	console.log("requestsView.define() - building the table ...");

        // do page title
        var pageTitle = document.createElement("div");
        pageTitle.innerHTML = "requests: ";
        pageTitle.id = "pagetitle";
        mainElem.appendChild(pageTitle);
        
        // mainElem.appendChild(document.createElement("br"));

        // set up the main table for requests
        var requestsTable = requestsView.setUpRequestsTable();
        var requestsDiv = document.createElement("div");
        requestsDiv.className = "requestscontent";
        
        requestsDiv.id = "requestscontentid";
        requestsDiv.appendChild(requestsTable);        
        mainElem.appendChild(requestsDiv);

        mainElem.appendChild(document.createElement("br"));

    }, // define()
    
    
    /*
     * create a table for displaying requests, this function
     * defines table header
     */
    setUpRequestsTable: function()
    {
	    var table = document.createElement("table");
	    // referenced when adding rows in the table
	    table.id = "requestsviewtable";
	    table.cellPadding = "5px"; // doesn't work when applied in style
	    // first row is header
	    // 0 - Date, 1 - Name, 2 - Type, 3 - Status, 4 - details 
	    var hRow = table.insertRow(0);
	    hRow.className = "boldtextandbackground";
	    hRow.insertCell(0).innerHTML = "&nbsp;"; // open|close request details toggle
	    hRow.insertCell(1).innerHTML = "Date";
	    hRow.insertCell(2).innerHTML = "Name";
	    hRow.insertCell(3).innerHTML = "Type";
	    hRow.insertCell(4).innerHTML = "Status";
	    hRow.insertCell(5).innerHTML = "details";
	    return table;
	    
    }, // setUpRequestsTable()
    
        
    addTableRow: function(tableRow, date, name, type, status, details)
    {
    	// multiplied by 1000 so that the argument is in milliseconds, not seconds
    	// (Python vs JavaScript discrepancy)
    	// var dateTime = new Date(parseInt(time * 1000)).toLocaleString();
    	// link to the couch document
    	
    	// 0 - Date, 1 - Name, 2 - Type, 3 - Status, 4 - details
    	// approx. 5x faster to process than row.insertCell(0).innerHTML = dateTime; ...
    	// however doing innerHTML <tr> on the entire table with 14 item never finishes ...
        tableRow.innerHTML = "<td>" + requestsView.closedCellInnerHTML + "</td>" + 
        	                 "<td>" + date + "</td>" +
    	   	                 "<td>" + name + "</td>" +
    	   	                 "<td>" + type + "</td>" +
    	   	                 "<td>" + status + "</td>" +
    	   	                 "<td>" + details + "</td>";
        tableRow.onmouseover = requestsView.requestTableMouseOverHandler;
        tableRow.onmouseout = requestsView.requestTableMouseOutHandler;
        // clickable open|closed toggle entire row (could also be on 
        // first cell tableRow.cells[0] only)
        // tableRow.onclick = requestsView.requestDetailsToggle;
        // detailsToggle - arbitrary flag
        tableRow.detailsToggle = "closed";
    }, // addTableRow()
    
    
    requestTableMouseOverHandler: function()
    {
    	this.className = "onHover";
    	
    }, // requestTableMouseOverHandler()
    
    
    requestTableMouseOutHandler: function()
    {	
    	this.className = this.requestArrayId % 2 === 0 ? "even" : "odd";   
    	
    }, // requestTableMouseOutHandler()
    
    
    /*
     * Adds rows for corresponding requests into the tableElem.
     */
    displayRequests: function(tableElem, data)
    {
	    var tableRow;
	    // 0 - Date, 1 - Name, 2 - Type, 3 - Status, 4 - details
		for (var i=0; i<data.rows.length; i++) 
		{
			var date = data.rows[i].doc["RequestDate"];
	        var name = data.rows[i].doc["RequestName"];
	        var type = data.rows[i].doc["RequestType"];
	        var status = data.rows[i].doc["RequestStatus"];
	        var details = "n/a";
	        var rowClass = i % 2 === 0 ? "even" : "odd";
	        tableRow = tableElem.insertRow(-1);
	        tableRow.className = rowClass;
	        tableRow.requestArrayId = i;
	        requestsView.addTableRow(tableRow, date, name, type, status, details);
		}
		// output number of displayed items
		var pageTitle = document.getElementById("pagetitle");
	    pageTitle.innerHTML = "Displayed requests: " + i;
		
    }, // displayRequests()    
    
    
    /*
     * Called to process data returned by the view.
     * Function is specified as callback for http query and processes result.
     */
    processRequestsData: function(requestData)
    {    	
    	var tableElem = document.getElementById("requestsviewtable");
    	var data = requestData["result"][0];
    	console.log("processRequestsData() rows in input data: " + 
    				data.rows.length + 
    			    " rows in current table: " + tableElem.rows.length);
    	var startTime = new Date().getTime();
    	
    	requestsView.displayRequests(tableElem, data);
    	
    	var endTime = new Date().getTime();
    	
    	// set the loading sign off
    	document.getElementById("loadingshadowdivid").className = "hidden";
    	utils.printTiming(startTime, endTime, "processRequestsData()");
    	
    	// check what to display at the bottom 'See more ' button
    	var loadMoreBottomDiv = document.getElementById("loadmorebottomdivid");
    	
    }, // processRequestsData()

    
    /*
     * Main function called upon page update.
     */
	update: function()
	{
    	var url = requestsView.mainAppUrl + "/data/request";
	    var options = {"method": "GET", "reloadPage": false};	    
	    var data = {};
	    utils.makeHttpRequest(url, requestsView.processRequestsData, data, options);
	    
	    // set the loading sign on, may check if (document.readyState == "loading")
	    document.getElementById("loadingshadowdivid").className = "displayed";
	} // update()

    
}; // requestsView