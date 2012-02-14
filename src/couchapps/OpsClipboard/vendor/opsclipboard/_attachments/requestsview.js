// requestsview.js
//
// presents a tabled list of requests


var requestsView = 
{
	mainUrl: null, // full URL to the couchapp
    
    // main page building & display
    // build a table in the supplied HTML element
    // elemId is parent elementId
    define: function(input)
    {
		utils.setUp(requestsView);
		var elemId = input.contentDivId;
        console.log("requestsView.define() - building the table");
        
        // do page title
        var pageTitle = document.createElement("div");
        pageTitle.innerHTML = "Requests View";
        pageTitle.id = "pagetitle";
        document.getElementById(elemId).appendChild(pageTitle);
  
        // create campaign selection filter
        var tableCampaignSelect = document.createElement("table");
        var row = tableCampaignSelect.insertRow(-1);
        row.insertCell(0).innerHTML = "Filter by Campaign";
        row.style.fontWeight = "bold";
        var campaignSelect = document.createElement("select");
        campaignSelect.id = "campaignselectid";
        campaignSelect.name = "campaignSelect";
    	var option = document.createElement("option");
    	option.text = "<unselected>";
    	campaignSelect.add(option, null);
    	campaignSelect.onchange = function() { requestsView.byCampaignSelectionUpdate(elemId); };
        row.insertCell(1).appendChild(campaignSelect);
        var row = tableCampaignSelect.insertRow(-1);
        document.getElementById(elemId).appendChild(tableCampaignSelect);
        document.getElementById(elemId).appendChild(document.createElement("br"));

        var table = requestsView.setUpRequestsTable();
        document.getElementById(elemId).appendChild(table);
    }, // define()
    
    
    setUpRequestsTable: function()
    {
	    var table = document.createElement("table");
	    table.id = "requestsviewtable";
	    // entire table style
	    table.style.border = "2px solid black";
	    table.style.textAlign = "center";
	    table.cellPadding = "5px";
	    table.rules = "cols"; // "all";
	    var header = table.createTHead();
	    // 0 - state, 1 - updated, 2 - request id (OpsClipboard link)
	    var hRow = header.insertRow(0);
	    hRow.style.fontWeight = "bold";
	    hRow.style.backgroundColor = "#DDDDDD";
	    hRow.insertCell(0).innerHTML = "OpsClipboard State";
	    hRow.insertCell(1).innerHTML = "Last Updated";
	    hRow.insertCell(2).innerHTML = "Request ID";
	    return table;
    }, // setUpRequestsTable()    

    
    addTableRow: function(reqId, state, docId, lastUpdated, rowColor)
    {
    	console.log("adding:" + state + "  " + lastUpdated + "  " + reqId + "  " + docId);
    	var updatedDateTime = new Date(parseInt(lastUpdated)).toLocaleString();
        var clipLink = "<a href=\"" + requestsView.mainUrl;
        clipLink += "index.html?object=requestShow&docId=" + docId + "\">" + reqId + "</a>";
    	table = document.getElementById("requestsviewtable");
    	var row = table.insertRow(-1);
    	row.style.backgroundColor = rowColor;
    	row.insertCell(0).innerHTML = state;
    	row.insertCell(1).innerHTML = updatedDateTime; 
    	row.insertCell(2).innerHTML = clipLink; 
    }, // addTableRow()
    
        
    processRequestsData: function(data)
    {
    	// this function is called to process results from 'campaign' view
    	// which returns a list of request sorted by the key which is campaign name
    	// this orders the list according to request names 
    	function sortFunc(a, b)
    	{
    		return a.value["request_id"] > b.value["request_id"];
    	};
    	data.rows.sort(sortFunc);	
		
    	for (i in data.rows) 
		{
			var reqId = data.rows[i].value['request_id'];
            var docId = data.rows[i].value['doc_id'];
            var state = data.rows[i].value['state'];
            var updated = data.rows[i].value['updated'];
            // alternate colours in table rows
            var rowColor = i % 2 === 0 ? "#FAFAFA" : "#E3E3E3";  
            requestsView.addTableRow(reqId, state, docId, updated, rowColor);
        }    	
    }, // processRequestsData()
        
    
    setCampaignSelect: function()
    {    	
    	var url = requestsView.mainUrl + "_view/campaign_ids";
    	var data = {"group": true};
    	var options = {"method": "GET", "reloadPage": false};
    	var campaignSelect = document.getElementById("campaignselectid");
    	utils.makeHttpRequest(url, function(data) 
    	{
    		for (i in data.rows) 
    		{
    	    	var campName = data.rows[i]["key"];
    	    	var option = document.createElement("option");
    	    	option.text = campName;
    	    	campaignSelect.add(option, null);
    	    }
    	}, data, options);	
    }, // setCampaignSelect()
    
    
    // load view from couch and populate page
    // this function is called from the HTML page, fills in the content
    // of the campaign drop-down menu
    update: function()
    {
    	var url = requestsView.mainUrl + "_view/campaign";
        var options = {"method": "GET", "reloadPage": false};
        var campaignOptions = document.getElementById("campaignselectid").options;
        utils.makeHttpRequest(url, requestsView.processRequestsData, null, options);        
        requestsView.setCampaignSelect();
    }, // update()
    
    
    // called on-change by campaign select drop-down menu
    // content of the drop-down menu is not modified here
    byCampaignSelectionUpdate: function(elemId)
    {
    	var url = requestsView.mainUrl + "_view/campaign";
        var options = {"method": "GET", "reloadPage": false};    	
    	var campaignOptions = document.getElementById("campaignselectid").options;

    	// get selected option from the drop-down menu and narrow the request
    	// view accordingly
    	var campaignName = campaignOptions[campaignOptions.selectedIndex].text;
    	console.log("selected campaign name: " + campaignName);
    	var data = null;
    	if (campaignName !== "<unselected>")
    	{
    		data = {"startkey": campaignName, "endkey": campaignName};
    	}
    	// remove current stuff from the table
    	// removing by row shall potentially work, but results are chaotic and disfunctional
    	// able.deleteRow(i);	
    	// replacing the whole element works ok
    	table = document.getElementById("requestsviewtable");
    	document.getElementById(elemId).removeChild(table);
    	var newTable = requestsView.setUpRequestsTable();
    	document.getElementById(elemId).appendChild(newTable);
        utils.makeHttpRequest(url, requestsView.processRequestsData, data, options);    	
    } // byCampaignSelectionUpdate()
    
    
} // requestsView