var bulkchange = {

    couchdb : null,
    mainpage: null,

    // util to bootstrap the couch API
    setCouchDB: function(){
        var dbname = document.location.href.split('/')[3];
        console.log("couchdb ref set...")
        this.couchdb = $.couch.db(dbname);
        this.mainpage = this.couchdb.uri + "_design/OpsClipboard/index.html";
    },
    

    
    changeState : function(docId, newState){
        var changeUpdate = bulkchange.couchdb.uri + "_design/OpsClipboard/_update/changestate/" + docId
        $.post(changeUpdate, { "selectState" : newState}, 
            function(data){console.log(data);}
        );
    },

    // trawl the table contents, find all the checked entries and the new states
    // and fire off the changeState requests
    submitBulkChange: function(){
        console.log("submitBulkChange");
        var rowsToSubmit = bulkchange.checkedRows();
        for (r in rowsToSubmit){
            bulkchange.changeState(rowsToSubmit[r].documentId, rowsToSubmit[r].newState);
        }
    },
    
    // find which table rows have the checked box checked
    checkedRows: function(){
        var results = []
        var table = document.getElementById("bulkrequest-table");
        for (i=1; i<table.rows.length; i++){
            var row = table.rows[i];
            var reqcell = row.getElementsByTagName("td")[0];
            var select = row.getElementsByTagName("select")[0];
            var check = row.getElementsByTagName("input")[0];
            
            
            
            if (check.checked){
                result = {}
                result.requestId = reqcell.requestId;
                result.documentId = reqcell.documentId;
                result.newState = null;
                if (select.selectedIndex >= 0){
                    var selected = select.options[select.selectedIndex];
                    result.newState = selected.value
                }
                if (result.newState != null){
                    results.push(result);
                } 
                
            }
        }
        return results;     
    },
    
    // build the bulk change table and submit button
    bulkchange : function(elemId){
        console.log("bulkchange");
        var table = document.createElement("table");
        table.id = "bulkrequest-table";
        var title = document.createElement("tr");
        var reqColumn = document.createElement("th");
        var stateColumn = document.createElement("th"); 
        var newStateColumn = document.createElement("th"); 
        var checkboxColumn = document.createElement("th");
        
        table.appendChild(title);
        title.appendChild(reqColumn);
        title.appendChild(stateColumn);
        title.appendChild(newStateColumn);
        title.appendChild(checkboxColumn);
        

        reqColumn.innerHTML = "Request ID";
        stateColumn.innerHTML = "Current State";
        newStateColumn.innerHTML = "New State";
        checkboxColumn.innerHTML = "Change State";
        
        
        document.getElementById(elemId).appendChild(table);
        
        var button = document.createElement("input");
        button.type = "button";
        button.value="Submit Changes";
        button.onclick = this.submitBulkChange;
        document.getElementById(elemId).appendChild(button);
      
        // main page link
        var backLink = document.createElement("div");
        backLink.innerHTML = "<a href=\"" + bulkchange.mainpage + "\">Main Page</a>";
        document.body.appendChild(backLink);
        
    },
    
    addTableRow : function(req, docId, state){
        var possibleStates = opsstates.states[state];
        var newRow = document.createElement("tr");
        newRow.name = "thisIsADataRow";
        var col1 = document.createElement("td");
        var col2 = document.createElement("td");
        var col3 = document.createElement("td");
        var col4 = document.createElement("td");
        
        col1.documentId = docId;
        col1.requestId = req;
        
        newRow.appendChild(col1);
        newRow.appendChild(col2);
        newRow.appendChild(col3);
        newRow.appendChild(col4);
                                
        var checkbox = document.createElement("input");
        checkbox.type = "checkbox";
        if ( possibleStates.length == 0){
            checkbox.disabled = true;
        }
        
        var menu = document.createElement("select");
        for (x in possibleStates){
            menu.options[x] = new Option(possibleStates[x], possibleStates[x], true, false);
        }
        
        col1.innerHTML = req;
        col2.innerHTML = state;
        col3.appendChild(menu);
        col4.appendChild(checkbox);
        
        document.getElementById("bulkrequest-table").appendChild(newRow);
        
    },
    
    // load the couch view and populate the table.
    // each table row is tagged with the request id that can be used to look up and modify the table
    // when bulk changes are committed
    update : function(){
         this.couchdb.view("OpsClipboard/request",{
                  success : function(data){
                      for (i in data.rows) {
                          var req = data.rows[i].key[0];
                          var doc = data.rows[i].value['doc'];
                          var state = data.rows[i].value['state'];
                          bulkchange.addTableRow(req, doc, state);
                          
                      }
                  }
              });
    }
    
    
}