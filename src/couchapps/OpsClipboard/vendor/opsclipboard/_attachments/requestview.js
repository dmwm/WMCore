
var requestview = {
    
    couchdb : null,
    mainpage : null, 
    
    // util to bootstrap the couch API
    setCouchDB: function(){
        var dbname = document.location.href.split('/')[3];
        console.log("couchdb ref set...")
        this.couchdb = $.couch.db(dbname);
        this.mainpage = this.couchdb.uri + "_design/OpsClipboard/index.html";
    },
    
    // main page building & display
    // build a table in the supplied HTML element
    requestview : function(elemId){
        console.log("requestview");
        var table = document.createElement("table");
        table.id = "requestview-table";
        var title = document.createElement("tr");
        var reqColumn = document.createElement("th");
        var stateColumn = document.createElement("th"); 
        var updateColumn = document.createElement("th"); 
        var linkColumn = document.createElement("th");
        
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
        backLink.innerHTML = "<a href=\"" + requestview.mainpage + "\">Main Page</a>";
        document.body.appendChild(backLink);
    },
    
    addTableRow : function(reqId, state, docId, lastUpdate){
        console.log("adding:" + reqId + " " + state + " " + docId + " " + lastUpdate);
        
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
        
        var cliplink = "<a href=\"" + requestview.couchdb.uri ;
        cliplink += "_design/OpsClipboard/_show/clipboardview/";
        cliplink += docId ;
        cliplink += "\">Clipboard</a>";
        
        col1.innerHTML = reqId;
        col2.innerHTML = state;
        col3.innerHTML = d.toLocaleString();
        col4.innerHTML = cliplink;
        
        document.getElementById("requestview-table").appendChild(newRow);
        
    },
    
    // load view from couch and populate page
    update : function(){
        this.couchdb.view("OpsClipboard/request",{
              success : function(data){
                  for (i in data.rows) {
                      var req = data.rows[i].key[0];
                      var doc = data.rows[i].value['doc'];
                      var state = data.rows[i].value['state'];
                      var updated = data.rows[i].value['updated'];
                      requestview.addTableRow(req,state, doc, updated);
                  }
              }
          });
    }
    
    
}