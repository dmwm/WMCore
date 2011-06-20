var campaignview = {
    
    couchdb : null,
    campaign : null,
    mainpage : null,
    
    // util to bootstrap the couch API
    setCouchDB: function(){
        var dbname = document.location.href.split('/')[3];
        console.log("couchdb ref set...")
        this.couchdb = $.couch.db(dbname);
        this.mainpage = this.couchdb.uri + "_design/OpsClipboard/index.html";
    },
    
    // parse the campaign from the URL
    setCampaign: function(){
        var campaign = campaignview.getQueryVariable("campaign");
        campaignview.campaign = campaign;
        console.log("campaign is "+ campaign);
    },
    
    // QND URL parsing to get a query argument
    getQueryVariable: function(variable) { 
      var query = window.location.search.substring(1); 
      var vars = query.split("&"); 
      for (var i=0;i<vars.length;i++) { 
        var pair = vars[i].split("="); 
        if (pair[0] == variable) { 
          return pair[1]; 
        } 
      } 
      return null;
    },
    
    // main page building & display
    // build a table in the supplied HTML element
    campaignview : function(elemId){
        if (campaignview.campaign == null){
            var errmsg = document.createElement("p");
            errmsg.innerHTML = "No Campaign Argument provided in URL\n";
            errmsg.innerHTML += "URL must contain ?campaign=CAMPAIGNNAME";
            document.getElementById(elemId).appendChild(errmsg);
            return
        }
        var table = document.createElement("table");
        table.id = "campaignview-table";
        var camptitle = document.createElement("tr");
        var camptext = document.createElement("th");
        camptext.colSpan = 4;
        camptext.innerHTML = "<h4>Campaign: "+ campaignview.campaign + "</h4>";
        camptitle.appendChild(camptext);
        
        
        var title = document.createElement("tr");
        var reqColumn = document.createElement("th");
        var stateColumn = document.createElement("th"); 
        var updateColumn = document.createElement("th"); 
        var linkColumn = document.createElement("th");
        
        table.appendChild(camptitle);
        table.appendChild(title);
        title.appendChild(reqColumn);
        title.appendChild(stateColumn);
        title.appendChild(updateColumn);
        title.appendChild(linkColumn);
        
        
        reqColumn.innerHTML = "Request ID";
        linkColumn.innerHTML = "Clipboard";
        updateColumn.innerHTML = "Last Updated";
        stateColumn.innerHTML = "Clipboard State";
        
        document.getElementById(elemId).appendChild(table);
        
        // main page link
        var backLink = document.createElement("div");
        backLink.innerHTML = "<a href=\"" + campaignview.mainpage + "\">Main Page</a>";
        document.body.appendChild(backLink);

        
    },
    addTableRow : function(reqId, state, docId, lastUpdate){
        
        
        var newRow = document.createElement("tr");
        var col1 = document.createElement("td");
        var col2 = document.createElement("td");
        var col3 = document.createElement("td");
        var col4 = document.createElement("td");
        
        newRow.appendChild(col1);
        newRow.appendChild(col2);
        newRow.appendChild(col3);
        newRow.appendChild(col4);
                        
        var t = parseInt(lastUpdate);
        var d = new Date(t);
        
        var cliplink = "<a href=\"" + campaignview.couchdb.uri ;
        cliplink += "_design/OpsClipboard/_show/clipboardview/";
        cliplink += docId ;
        cliplink += "\">Clipboard</a>";
        
        col1.innerHTML = reqId;
        col2.innerHTML = state;
        col3.innerHTML = d.toLocaleString();
        col4.innerHTML = cliplink;
        
        document.getElementById("campaignview-table").appendChild(newRow);
        
    },
    update : function(){
        if (campaignview.campaign == null){
            return;
        }
        this.couchdb.view("OpsClipboard/campaign", {
            startkey : [campaignview.campaign], endkey:[campaignview.campaign],
              success : function(data){
                  for (i in data.rows) {
                      var req = data.rows[i].value['request'];
                      var doc = data.rows[i].value['doc'];
                      var state = data.rows[i].value['state'];
                      var updated = data.rows[i].value['updated'];
                      campaignview.addTableRow(req,state, doc, updated);
                  }
              }
          });
    },

    // generate a list of known campaigns that provide a link into the campaign view for that campaign
    campaignlist : function(elem){
        var table = document.createElement("table");
        table.id = "campaignview-table";
        var titlerow = document.createElement("tr");
        var titletext = document.createElement("th");
        titletext.innerHTML = "Campaign"
        table.appendChild(titlerow);
        titlerow.appendChild(titletext);
        
        document.getElementById(elem).appendChild(table);
        // main page link
        var backLink = document.createElement("div");
        backLink.innerHTML = "<a href=\"" + campaignview.mainpage + "\">Main Page</a>";
        document.body.appendChild(backLink);
        
        this.couchdb.view("OpsClipboard/campaignlist", {
            group : true,
              success : function(data){
                  for (i in data.rows) {
                      var campname = data.rows[i]['key'];
                      var newRow = document.createElement("tr");
                      var newEntry = document.createElement("td");
                      var entryLink = "<a href=\"" + campaignview.couchdb.uri ;
                      entryLink += "_design/OpsClipboard/campaignview.html?campaign=" + campname
                      entryLink += "\">" + campname + "</a>";
                      newEntry.innerHTML = entryLink;
                      newRow.appendChild(newEntry);
                      document.getElementById("campaignview-table").appendChild(newRow);
                      console.log(campname);
                  }
              }
          });
        
        
    }
    
}