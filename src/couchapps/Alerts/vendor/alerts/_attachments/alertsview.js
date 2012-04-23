// alertsview.js
//
// presents a tabled list of alerts


var alertsView = 
{
    mainUrl: null, // full URL to the couchapp
    
    // main page building & display
    // build a table in the supplied HTML element
    // mainElemId is parent elementId
    define: function(input)
    {
		utils.setUp(alertsView);
		var mainElemId = input.contentDivId;
		console.log("alertsView.define() - building the table");
		
        // do page title
        var pageTitle = document.createElement("div");
        pageTitle.innerHTML = "Displayed Alerts: ";
        pageTitle.id = "pagetitle";
        document.getElementById(mainElemId).appendChild(pageTitle);
        
        // create by HostName selection filter
        var input = {"title": "Filter by HostName",
        		     "selectorId": "hostnameselectid",
        		     "handler": alertsView.bySelectionUpdate,
        		     "mainElemId": mainElemId};        		  
        var table = alertsView.setUpSelectionFilter(input);
        document.getElementById(mainElemId).appendChild(table);
        
        document.getElementById(mainElemId).appendChild(document.createElement("br"));

        // create by Source selection filter
        var input = {"title": "Filter by Source",
		   		     "selectorId": "sourceselectid",
		   		     "handler": alertsView.bySelectionUpdate,
		   		     "mainElemId": mainElemId};        	
        var table = alertsView.setUpSelectionFilter(input);
        document.getElementById(mainElemId).appendChild(table);
       
        document.getElementById(mainElemId).appendChild(document.createElement("br"));
        
        // set up the main table for alerts
        var table = alertsView.setUpAlertsTable();
        // document.getElementById(mainElemId).appendChild(table);
        
        var alertsDiv = document.createElement("div");
        alertsDiv.id = "alertscontent";
        alertsDiv.appendChild(table);
        
        document.getElementById(mainElemId).appendChild(alertsDiv);
        
        document.getElementById(mainElemId).appendChild(document.createElement("br"));
    }, // define()
    
    
    // function creates a table which hosts filter title and the selection
    // drop-down box itself. returns the table definition.
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
    	selector.onchange = function() { input["handler"](input["mainElemId"]); };
        row.insertCell(1).appendChild(selector);
        return table;
    }, // setUpHostNameSelectionTable()
    	
    
    setUpAlertsTable: function()
    {
	    var table = document.createElement("table");
	    table.id = "alertsviewtable";
	    table.rules = "cols"; // doesn't work when applied in style
	    table.cellPadding = "8px"; // doesn't work when applied in style
	    var header = table.createTHead();
	    // 0 - Timestamp, 1 - HostName, 2 - AgentName, 3 - Component, 4 - Source 
	    var hRow = header.insertRow(0);
	    hRow.className = "boldtextandbackground";
	    hRow.insertCell(0).innerHTML = "Timestamp";
	    hRow.insertCell(1).innerHTML = "Host Name";
	    hRow.insertCell(2).innerHTML = "Agent Name";
	    hRow.insertCell(3).innerHTML = "Component";
	    hRow.insertCell(4).innerHTML = "Source";	    
	    return table;
    }, // setUpAlertsTable()
    

    /* called on-change by HostName select drop-down menu to narrow the view
     * called on-change by Source select drop-down menu to narrow the view
     * selection only by HostName corresponds to byhostname view
     * selection only by Source corresponds to by source view
     * seleciton by both HostName and Source together corresponds to byhostnamebysource view
     * content of the drop-down menus is not modified here
     */
    bySelectionUpdate: function(mainElemId)
    {
    	function getData(selectorId)
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
    	var host = getData("hostnameselectid");
    	// check Source selection
    	var source = getData("sourceselectid");
    	var url = alertsView.mainUrl;
    	var data = null;
    	if (host && !source)
    	{
    		data = {"startkey": host, "endkey": host};
    		url += "_view/byhostname";
    	}
    	else if (!host && source)
    	{
    		data = {"startkey": source, "endkey": source};
    		url += "_view/bysource";
    	}
    	else if (host && source)
    	{
    	
    		data = {"startkey": [host, source], "endkey": [host, source]};
    		url += "_view/byhostnamebysource";
    	}
    	else
    	{
    		url += "_view/all";
    	}
        var options = {"method": "GET", "reloadPage": false};    	

    	// remove current stuff from the table
    	// removing by row shall potentially work, but results are chaotic and disfunctional
    	// able.deleteRow(i);	
    	// replacing the whole element works ok
    	table = document.getElementById("alertsviewtable");
    	document.getElementById(mainElemId).removeChild(table);
    	var newTable = alertsView.setUpAlertsTable();
    	document.getElementById(mainElemId).appendChild(newTable);
        utils.makeHttpRequest(url, alertsView.processAlertsData, data, options);    	    	
    }, // bySelectionUpdate()
        
    
    addTableRow: function(time, host, agent, component, docId, source, rowClass)
    {
    	// multiplied by 1000 so that the argument is in milliseconds, not seconds
    	// (Python vs JavaScript discrepancy)
    	var dateTime = new Date(parseInt(time * 1000)).toLocaleString();
    	// link to the couch document representing the alert
    	var url = alertsView.mainUrl.split("_design")[0] + docId;
        var couchLink = "<a href=\"" + url + "\" target=_blank>" + source + "</a>";
    	table = document.getElementById("alertsviewtable");
    	var row = table.insertRow(-1);
    	row.className = rowClass;
    	// 0 - Timestamp, 1 - HostName, 2 - AgentName, 3 - Component, 4 - Source
    	row.insertCell(0).innerHTML = dateTime;
    	row.insertCell(1).innerHTML = host; 
    	row.insertCell(2).innerHTML = agent; 
    	row.insertCell(3).innerHTML = component;
    	row.insertCell(4).innerHTML = couchLink;
    }, // addTableRow()

    
    // this function is called to process results of the all view (all alerts)
    processAlertsData: function(data)
    {
    	console.log("processAlertsData()");
    	// output number of returned alerts
    	var pageTitle = document.getElementById("pagetitle");
        pageTitle.innerHTML = "Displayed Alerts: " + data.rows.length;
    	// 0 - Timestamp, 1 - HostName, 2 - AgentName, 3 - Component, 4 - Source
    	for (i in data.rows) 
		{
			var time = data.rows[i].value["Timestamp"];
            var host = data.rows[i].value["HostName"];
            var agent = data.rows[i].value["AgentName"];
            var component = data.rows[i].value["Component"];
            var source = data.rows[i].value["Source"];
            var docId = data.rows[i].value["_id"];
            var rowClass = i % 2 === 0 ? "even" : "odd";  
            alertsView.addTableRow(time, host, agent, component, docId, source, rowClass);
        }    	
    }, // processAlertsData()
    
    
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
    
    
	// load view from couch and populate page
	// this function is called from the HTML page, fills in the content
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
	} // update()

		
} // alertsView