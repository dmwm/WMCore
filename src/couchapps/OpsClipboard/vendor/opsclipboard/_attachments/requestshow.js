// requestshow.js
//
// presents details of a particular request
// allows for adding description to requests and changing request status

// in the processData function used as a callback
// using this.variableName leads to undefined - use with module name OK


var requestShow = 
{
	mainUrl: null, // full URL to the couchapp, gets set up in the utils.setUp()
	documentId: null, // couch doc id
    // ids of HTML elements (key in this dictionary) and element titles for request
    // details section of the page
    reqDetailsTableElements: {"currDocIdCellId": "CouchDB Doc ID:",
    		                  "currStateCellId": "Current State:",
    		                  "createdCellId": "Created:",
    		                  "lastUpdatedCellId": "Last Updated:",
    		                  "campaignNameCellId": "Campaign Name:",
    		                  "requestNameCellId": "Request Name:",
    		                  "numDescCellId": "# Descriptions:"}, 

    		                  
    // main function that draws the page elements and generates the forms
    define: function(input)
    {
        utils.setUp(requestShow);        
    	// id of the div element into which this content will be defined
    	var requestShowPanelDivId = input.contentDivId;
    	requestShow.documentId = input.docId
    	console.log("requestShow.define() - request details");
    	
        // do page title
        var pageTitle = document.createElement("div");
        pageTitle.id = "pagetitle";
        document.getElementById(requestShowPanelDivId).appendChild(pageTitle);

        // left-hand side table with all request details, but descriptions (will be on the right)
        var detailsTable = document.createElement("table");
        detailsTable.style.backgroundColor = "#EBEBEB";
        detailsTable.style.textAlign = "left";
        detailsTable.cellPadding = "7px";        
        // construct a table with request details
        // key to the dictionary is element id name
        for (id in requestShow.reqDetailsTableElements)
        {
        	var row = detailsTable.insertRow(-1);
            row.insertCell(0).innerHTML = requestShow.reqDetailsTableElements[id];
            row.cells[0].className = "requestdetailstitle";
            row.insertCell(1); // will later hold the value
            row.cells[1].id = id;        	
        }
        // add change state row (label, listbox, form ...)
        var row = detailsTable.insertRow(-1);
        row.insertCell(0).innerHTML = "Change State:";
        row.cells[0].className = "requestdetailstitle";
        var stateMenu = document.createElement("select");
        stateMenu.id = "selectState";
        stateMenu.name = "selectState";
        var submitState = document.createElement("input");
        submitState.type = "button";
        submitState.value = "Change";
        submitState.onclick = requestShow.submitNewState;
        var stateForm = document.createElement("form");
        stateForm.id = "stateForm";
        stateForm.name = "stateForm";
        stateForm.appendChild(stateMenu);
        stateForm.appendChild(submitState);
        row.insertCell(1);
        row.cells[1].appendChild(stateForm);
        
        // right-hand side table with descriptions / notes                
        var descTable = document.createElement("table");
        descTable.style.backgroundColor = "#EBEBEB";
        descTable.cellPadding = "7px";
        // TODO
        // alignment doesn't seem to work on the right-hand side - starting from the top
        // likely incorrectly used ...
        // descTable.style.verticalAlign = "top";
        descTable.style.float = "top";
        
        var descriptionForm = document.createElement("form")
        descriptionForm.id = "descriptionForm";
        descriptionForm.name = "descriptionForm";
                
        var row = descTable.insertRow(-1);
        row.insertCell(0);
        var descLabel = document.createElement("div");
        descLabel.innerHTML = "Previous descriptions / notes:";
        descLabel.className = "requestdetailstitle";
        row.cells[0].appendChild(descLabel);
        // text area for previous descriptions (those loaded from couch)
        var prevDescTextArea = document.createElement("textarea");
        prevDescTextArea.readOnly = true;
        prevDescTextArea.style.backgroundColor = "#FAFAFA";
        prevDescTextArea.id = "prevDescTextAreaId";
        prevDescTextArea.style.width  = "350px";
        prevDescTextArea.style.height = "200px";
        row.cells[0].appendChild(prevDescTextArea);
        // text area for new request descriptions / notes
        var newDescTextArea = document.createElement("textarea");
        newDescTextArea.style.width  = "350px";
        newDescTextArea.style.height = "70px";
        newDescTextArea.value = "Add a note here..."
        newDescTextArea.id = "newDescriptionTextAreaId";
        newDescTextArea.name = "newDescriptionTextAreaId";
        // auto highlight the initial text to make it easy to overwrite
        // newDescTextArea.onclick = function(){newDescTextArea.select();}
        // erase on clicking the area
        newDescTextArea.onclick = function(){newDescTextArea.value = "";}
        // add to a form:
        var row = descTable.insertRow(-1);        
        row.insertCell(0).appendChild(newDescTextArea);
        descriptionForm.appendChild(newDescTextArea);
        
        // keep at this position in the sequence of adding elements ...
        row.cells[0].appendChild(descriptionForm);
                
        // button to trigger form post
        var submitDesc = document.createElement("input");
        submitDesc.type = "button";
        submitDesc.value = "Add Description";
        submitDesc.onclick = requestShow.submitNewDesc;
        row.cells[0].appendChild(submitDesc);
        
        // main table
        var mainTable = document.createElement("table");
        mainTable.style.backgroundColor = "#D9D9D9";
        // TODO
        // alignment doesn't seem to work on the right-hand side - starting from the top
        // likely incorrectly used ...        
        // mainTable.style.verticalAlign = "top";
        mainTable.style.float = "top";
        mainTable.cellPadding = "7px";
        mainTable.cellSpacing = "7px";
        var row = mainTable.insertRow(-1);
        // insert both sub-tables
        row.insertCell(0).appendChild(detailsTable);
        row.insertCell(1).appendChild(descTable);
                
        document.getElementById(requestShowPanelDivId).appendChild(mainTable);
        
        // TODO
        // perhaps the solution to align label and the listbox ...
        // display the current state in a labelled div
        // var stateLabel = document.createElement("label");
        // stateLabel.for = "currState";
        // stateLabel.innerHTML = "<p>Current State:</p>";
        // var currentState = document.createElement("div")
    }, // define()
    

    // submit the change state form to the update handler when the
    // "Change State" button is clicked 
    submitNewState: function()
    {
    	// submit the state change form to the update handler
        var form = document.getElementById("stateForm");
        var url = requestShow.mainUrl + "_update/changestate/" + requestShow.documentId;
        var newState = form.selectState.value;
        console.log("submitNewState url: " + url + " submitNewState: " + newState);
        if (newState == "")
        {
        	return;
        } 
        else 
        {
        	var data = {"newState" : newState};
        	var options = {"method": "POST", "reloadPage": true};
        	utils.makeHttpRequest(url, null, data, options);
        }
    }, // submitNewState()
    
    
    // submit the description form to the update handler
    // called when the "Add Description" button is clicked
    submitNewDesc : function()
    {
        var form = document.getElementById("descriptionForm")
        var url = requestShow.mainUrl + "_update/adddescription/" + requestShow.documentId;
        var data = {"newDescription": form.newDescriptionTextAreaId.value};
        var options = {"method": "POST", "reloadPage": true};
        utils.makeHttpRequest(url, null, data, options);
    }, // submitNewDesc()
            
        
    // populate the state change menu with the list of potential states
    populatePotentialStates: function(statesList)
    {
        var stateMenu = document.getElementById("selectState");
        for (x in statesList)
        {
            stateMenu.options[x] = new Option(statesList[x], statesList[x], true, false);
        } 
    }, // populatePotentialStates()
    
    
    // populate the request details table with data
    // data is also a dictionary using the same keys as reqDetailsTableElements
    populateRequestDetailsTable: function(data) 
    {	
    	console.log("populateRequestDetailsTable ...")
        for (id in requestShow.reqDetailsTableElements)
        {
        	var cell = document.getElementById(id);
        	cell.innerHTML = data[id];        	
        }
    }, // populateRequestDetailsTable()
    
    
    // populate the description display with the current list of time sorted descriptions
    populateExistingDescs: function(descs)
    {
    	descs.reverse(); // latest description appears at the bottom of the textarea
        var descriptions = "";
        // example descs is list of descriptions:
        // "description":[{"info":"Initial injection by the RequestManager","timestamp":1322226547740.0}]
        for (index in descs)
        {        	
        	var dt = new Date(parseInt(descs[index].timestamp)).toLocaleString();
            descriptions += "\n" + dt + ":\n";
            descriptions += descs[index].info + "\n";
        }
        var descDisplay = document.getElementById("prevDescTextAreaId");
        descDisplay.value = descriptions;
    }, // populateExistingDescs()
        
    
    processData: function(couchDoc)
    {
		console.log("updating page on couch doc id: " + requestShow.documentId);
		var pageTitle = document.getElementById("pagetitle");
		pageTitle.innerHTML = "Request \"" + couchDoc.request.request_id + "\"";
		var data = {};		
		// keys used here has to agree with reqDetailsTableElements	
		var url = requestShow.mainUrl.split("_design")[0] + requestShow.documentId;
		// provide link into couchdb along with the docId
		var couchLink = "<a href=" + url + " target=_blank>" + requestShow.documentId + "</a>"; 
		data["currDocIdCellId"] = couchLink;
		data["currStateCellId"] = couchDoc.state;
		data["createdCellId"] = new Date(parseInt(couchDoc.created)).toLocaleString(); 
		data["lastUpdatedCellId"] = new Date(parseInt(couchDoc.timestamp)).toLocaleString(); 	
		data["campaignNameCellId"] = couchDoc.request.campaign_id;
		data["requestNameCellId"] = couchDoc.request.request_id;
		data["numDescCellId"] = couchDoc.description.length;
		requestShow.populateRequestDetailsTable(data);
		requestShow.populateExistingDescs(couchDoc.description); 
		requestShow.populatePotentialStates(requestStatus.statusList[couchDoc.state]);
    }, // processData()
    
        
    // update function
    // retrieve the latest version of the doc from couch and populate
    // the displays and form fields 
    update: function()
    {    	
    	// URL to get: main URL without _design/CouchAppName plus docId
        var url = requestShow.mainUrl.split("_design")[0] + requestShow.documentId;
        var options = {"method": "GET", "reloadPage": false};
        utils.makeHttpRequest(url, requestShow.processData, null, options);
    }, // update() 
    
} // requestShow