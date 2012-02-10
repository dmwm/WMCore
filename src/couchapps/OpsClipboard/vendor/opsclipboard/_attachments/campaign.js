// campaign.js
//
// lists campaigns
// requests view by campaign


var campaign =
{
	mainUrl: null, // full URL to the couchapp
	campaignToQuery: null, // communicate the name of the campaign to query
    
    setUp: function()
    { 			
		utils.checkAndSetConsole();
		campaign.mainUrl = utils.getMainUrl(document.location.href);
    }, // setUp()
    
    
    // parse the campaign from the URL
    setCampaign: function()
    {
        campaign.campaignToQuery = campaign.getQueryVariable("campaign");
        console.log("campaign is "+ campaign.campaignToQuery);
    }, // setCampaign()
    
    
    // TODO
    // try to find better solution
    // QND URL parsing to get a query argument
    getQueryVariable: function(variable) 
    { 
      var query = window.location.search.substring(1); 
      var vars = query.split("&"); 
      for (var i=0; i<vars.length; i++)
      {
    	  var pair = vars[i].split("=");
    	  if (pair[0] == variable) 
    	  { 
    		  return pair[1]; 
    	  } 
      } 
      return null;
    }, // getQueryVariable()
    
    
    requestsViewByCampaign: function(elemId)
    {
    	if (campaign.campaignToQuery == null)
    	{
    		var errmsg = document.createElement("p");
            errmsg.innerHTML = "No <b>campaign</b> Argument provided in URL\n";
            errmsg.innerHTML += "URL must contain ?campaign=CAMPAIGNNAME";
            document.getElementById(elemId).appendChild(errmsg);
            return;
        }
    	
        var table = document.createElement("table");
        table.id = "campaignviewtableid";
        // entire table style
        table.style.border = "2px solid black";
        table.style.textAlign = "center";
        table.cellPadding = "5px";
        table.rules = "cols"; // "all";
        // do a table title and header - spanned cells above ...        
        var header = table.createTHead();
        var hRow = header.insertRow(0);
        hRow.style.backgroundColor = "#F5F5F5";
        var titleCell = hRow.insertCell(0);
        titleCell.colSpan = 3;
        titleCell.innerHTML = "<h4>Campaign: "+ campaign.campaignToQuery + "</h4>"; 
        // 0 - state, 1 - updated, 2 - request id (OpsClipboard link)
        var hRow = header.insertRow(1);
        hRow.style.fontWeight = "bold";
        hRow.style.backgroundColor = "#DDDDDD";
        hRow.insertCell(0).innerHTML = "OpsClipboard State";
        hRow.insertCell(1).innerHTML = "Last Updated";
        hRow.insertCell(2).innerHTML = "Request ID";
        document.getElementById(elemId).appendChild(table);
        
        utils.addPageLink(campaign.mainUrl + "index.html", "Main Page");
    }, // requestsViewByCampaign()
    
    
    addTableRow: function(reqId, state, docId, lastUpdated, rowColor)
    {    	
    	console.log("adding:" + state + "  " + lastUpdated + "  " + reqId + "  " + docId);
    	var updatedDateTime = new Date(parseInt(lastUpdated)).toLocaleString();
        var clipLink = "<a href=\"" + campaign.mainUrl;
        clipLink += "_show/request/" + docId + "\">" + reqId + "</a>";
    	table = document.getElementById("campaignviewtableid");
    	var row = table.insertRow(-1);
    	row.style.backgroundColor = rowColor;
    	row.insertCell(0).innerHTML = state;
    	row.insertCell(1).innerHTML = updatedDateTime; 
    	row.insertCell(2).innerHTML = clipLink; 
    }, // addTableRow()

    
    processDataByCampaign: function(data)
    {
		for (i in data.rows) 
		{
			var reqId = data.rows[i].value['request_id'];
            var docId = data.rows[i].value['doc_id'];
            var state = data.rows[i].value['state'];
            var updated = data.rows[i].value['updated'];
            // alternate colours in table rows
            var rowColor = i % 2 === 0 ? "#FAFAFA" : "#E3E3E3";  
            campaign.addTableRow(reqId, state, docId, updated, rowColor);
          }    	
    }, // processDataByCampaign()
    
    	
    // load view from couch and populate page
    requestsViewByCampaignUpdate: function()
    {
    	if (campaign.campaignToQuery == null)
    	{
            return;
        }
    	var url = campaign.mainUrl + "_view/campaign";
    	var data = {"startkey": campaign.campaignToQuery,
    			    "endkey": campaign.campaignToQuery};
        var options = {"method": "GET", "reloadPage": false};
        utils.makeHttpRequest(url, campaign.processDataByCampaign, data, options); 
    }, // requestsViewByCampaignUpdate()
            
    
    // generate a list of known campaigns that provide a link into
    // the campaign view for that campaign
    campaignList: function(elemId)
    {
    	console.log("querying couchapp view 'campaign_ids' ...");
    	var url = campaign.mainUrl + "_view/campaign_ids";
    	var data = {"group": true};
    	var options = {"method": "GET", "reloadPage": false};
    	// due to variables sharing, it's not straightforward how to move
    	// the callback into a separate function
    	utils.makeHttpRequest(url, function(data) 
    	{
    		for (i in data.rows) 
    		{
    	    	var listPanel = document.getElementById(elemId).appendChild(document.createElement("div"));
    	    	var listElem = listPanel.appendChild(document.createElement("ul"));
    	    	// if "group" options is not passed, the structure of response will change
    	    	var campName = data.rows[i]["key"];
    			console.log(campName);
                var link = "<a href=\"" + campaign.mainUrl;
                link += "requestsviewbycampaign.html?campaign=" + campName;
                link += "\">" + campName + "</a>";
                var listItem = document.createElement("li");
                listItem.innerHTML = link;
                listElem.appendChild(listItem);			
    	    }
    	}, data, options);    	

    	utils.addPageLink(campaign.mainUrl + "index.html", "Main Page");
    } // campaignList()
    
} // campaign()