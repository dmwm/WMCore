// namespace variable for all acdc display libraries
var acdc = {
    // reference for the couch db instance
    couchdb : null,
    
    // reference to dataTable;
    dataTable : null,
    
    //cache for a collection summary
    currentCollection: null,
    
    //tree reference 
    treeView : null,
    filesetNode: null, 
    //
    // Use this to provide the couch db API instance that these tools will use 
    //
    setCouchDB: function(couchRef){
        console.log("couchdb ref set...")
        this.couchdb = couchRef;
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
    
    // draw data table for collection view
    buildCollectionDataTable: function(tableElem){
      var myColumnDefs = [
            {key:"username" , resizeable:true,sortable:true},
            {key:"group",resizeable:true, sortable:true},
            {key:"collection",resizeable:true, sortable:true},
            {key:"document", resizable:true, sortable:false}
        ];
    
        // backend data source for the table is a JavaScript array that starts empty
        var myDataSource = new YAHOO.util.DataSource([]);
        myDataSource.responseType = YAHOO.util.DataSource.TYPE_JSARRAY;
        myDataSource.responseSchema = {
            fields: ["username","group","collection", "document"]
        };
    
        // the actual table
        var myDataTable = new YAHOO.widget.DataTable(tableElem,
                myColumnDefs, myDataSource, {"width" : 950});
        // set the groupuser.dataTable reference so that the onClick response can manipulate it
        acdc.dataTable = myDataTable;
        console.log("dataTable created...");
        
    },
    // populate collection table from couch view
    fillCollectionTable: function(){
        acdc.couchdb.view("ACDC/collection_name",{
              success : function(data){
                  // clean out existing rows in the display table
                  if(acdc.dataTable.getRecordSet().getLength() > 0) { 
                      acdc.dataTable.deleteRows(0, acdc.dataTable.getRecordSet().getLength()); 
                  }
                  for (i in data.rows){
                      // add new data rows to the table 
                      //configcache.dataTable.addRow({"group": data.rows[i].key[0], "username": data.rows[i].key[1], "document" : data.rows[i]['value']['config_doc'], "label" : data.rows[i]['value']['config_label']})
                      console.log(data.rows[i]);
                      var group = data.rows[i]['key'][1];
                      var user  = data.rows[i]['key'][2];
                      var name  = data.rows[i]['key'][0];
                      var doc   = data.rows[i]['value']['_id'];
                      var collLink = "<a href=\"" + acdc.couchdb.uri + "_design/ACDC/displaycollection.html" + "?group=" + group + "&user=" + user + "&collection=" + name + "\">" + doc + "</a>"; 
                      acdc.dataTable.addRow(
                          { "group" : group, "username" : user, "collection" : name, "document" : collLink}
                          );
                  }
              }
          });
    },
    
    // display the details of a single collection, getting the collection document ID from the URL parameters
    displayCollection: function() {
        console.log("displayCollection called");
        var collName = acdc.getQueryVariable("collection");
        var groupName = acdc.getQueryVariable("group");
        var userName = acdc.getQueryVariable("user");
        acdc.currentCollection = {};
        acdc.currentCollection["collection"] = collName;
        acdc.currentCollection["group"] = groupName;
        acdc.currentCollection["user"] = userName;
        acdc.currentCollection["filesets"] = [];
        
        if (collName == null){
            alert("No collection argument provided in the URL");
        }
        if (groupName == null){
            alert("No group argument provided in the URL");
        }
        if (userName == null){
            alert("No user argument provided in the URL");
        }
        console.log(collName + " " + groupName + " " + userName);
        acdc.couchdb.view("ACDC/collection_name",{
            startkey : [collName, groupName, userName], endkey:[collName, groupName, userName],
            success : function(data){
                acdc.currentCollection['id']  = data.rows[0]["id"];
                acdc.collectionFilesetSummary();
            } 
        });
        
    },
    
    // using information in acdc.currentCollection, call the view collection_fileset_summary and
    // add the filesets to currentCollection['filesets']
    collectionFilesetSummary: function(){
        acdc.couchdb.view("ACDC/collection_fileset_summary",{
            startkey : [acdc.currentCollection["id"]], endkey:[acdc.currentCollection["id"]],
            success : function(data){
                console.log("loading coll fileset summary...");
                for (i in data.rows){
                    acdc.currentCollection['filesets'].push(data.rows[i]['value']);
                    acdc.dataTable.addRow(
                          { "dataset" : data.rows[i]['value']['dataset'], 
                            "files" : data.rows[i]['value']['files'], 
                            "document" : data.rows[i]['value']['id'] }
                          );
                }
                
            }
        });
    },
    
    // draw data table for collection view
    buildFilesetSummaryDataTable: function(tableElem){
      var myColumnDefs = [
            {key:"dataset",resizeable:true, sortable:true},
            {key:"files",resizeable:true, sortable:true},
            {key:"document", resizable:true, sortable:false}
        ];
    
        // backend data source for the table is a JavaScript array that starts empty
        var myDataSource = new YAHOO.util.DataSource([]);
        myDataSource.responseType = YAHOO.util.DataSource.TYPE_JSARRAY;
        myDataSource.responseSchema = {
            fields: ["dataset", "files",  "id"]
        };
    
        // the actual table
        var myDataTable = new YAHOO.widget.DataTable(tableElem,
                myColumnDefs, myDataSource, {"width" : 950});
        // set the groupuser.dataTable reference so that the onClick response can manipulate it
        acdc.dataTable = myDataTable;
        console.log("dataTable created...");

    },
    
    buildFilesetDataTable: function(tableElem){
      console.log("buildFilesetDataTable");  
      var myColumnDefs = [
            {key:"username" , resizeable:true,sortable:true},
            {key:"group",resizeable:true, sortable:true},
            {key:"collection",resizeable:true, sortable:true},
            {key:"dataset",resizeable:true, sortable:false},
            {key:"document", resizable:true, sortable:false}
        ];
    
        // backend data source for the table is a JavaScript array that starts empty
        var myDataSource = new YAHOO.util.DataSource([]);
        myDataSource.responseType = YAHOO.util.DataSource.TYPE_JSARRAY;
        myDataSource.responseSchema = {
            fields: ["username","group","collection", "dataset", "document"]
        };
    
        // the actual table
        var myDataTable = new YAHOO.widget.DataTable(tableElem,
                myColumnDefs, myDataSource, {"width" : 950});
        // set the groupuser.dataTable reference so that the onClick response can manipulate it
        acdc.dataTable = myDataTable;
        console.log("dataTable created...");
    },
    
    fillFilesetTable: function(){
      console.log("fillFilesetTable");  
      acdc.couchdb.view("ACDC/fileset_owner_coll_dataset",{
            success : function(data){
                // clean out existing rows in the display table
                if(acdc.dataTable.getRecordSet().getLength() > 0) { 
                    acdc.dataTable.deleteRows(0, acdc.dataTable.getRecordSet().getLength()); 
                }
                for (i in data.rows){
                    console.log(data.rows[i]);
                    var docLink = "<a href=\"" + acdc.couchdb.uri + "_design/ACDC/displayfileset.html?document=" + data.rows[i]['value'] + "\">" + data.rows[i]['value'] + "</a>";
                    acdc.dataTable.addRow( { "group" : data.rows[i].key[0],
                                             "username" : data.rows[i].key[1],
                                             "collection" : data.rows[i].key[2],
                                             "dataset" : data.rows[i].key[3],
                                             "document" : docLink} )  ;
                }
            }
        });
    },
    
    //
    // lazy answer to displaying a chunk of JSON: turn it into a tree
    //
    buildFilesTreeView: function(treeElem){
        acdc.treeView = new YAHOO.widget.TreeView(treeElem); 
        acdc.filesetNode = new YAHOO.widget.TextNode("Fileset", acdc.treeView.getRoot(), false);
        acdc.treeView.draw();
        
        
        
    },
    
    //
    // function to turn a dictionary like JSON object into a TextNode in the tree
    // added to the node instance provided, will be recursively invoked for children in the object 
    buildTreeBranch: function(node,  objectRef){
        for ( o in objectRef) {
            if (typeof(objectRef[o]) == "object"){
                newNode = new YAHOO.widget.TextNode(o, node, false);                
                acdc.buildTreeBranch(newNode, objectRef[o]);
            } else {
                newNode = new YAHOO.widget.TextNode(o, node, false);
                valueNode = new YAHOO.widget.TextNode(objectRef[o], newNode,  true);                
            }
            
        }
    },
    
    
    // display the contents of a fileset as a table
    buildFilesDataTable: function(tableElem){
        console.log("buildFilesDataTable");
        var myColumnDefs = [
          {key:"lfn" , resizeable:true,sortable:true},
          {key: "events", resizeable: true, sortable:true},
          {key: "locations", resizeable: true, sortable:true},
          {key: "size", resizeable: true, sortable:true},
          ];
        var myDataSource = new YAHOO.util.DataSource([]);
        myDataSource.responseType = YAHOO.util.DataSource.TYPE_JSARRAY;
        myDataSource.responseSchema = {
              fields: ["lfn", "events", "size", "locations"]
        };
      
        // the actual table
        var myDataTable = new YAHOO.widget.DataTable(tableElem,
              myColumnDefs, myDataSource, {"width" : 950});
        // set the groupuser.dataTable reference so that the onClick response can manipulate it
        acdc.dataTable = myDataTable;      
    },
    
    displayFileset: function(){
        console.log("displayFileset");
        var docId = acdc.getQueryVariable("document");
        if (docId == null){
            alert('document argument not provided in URL');
            return
        }
        acdc.couchdb.openDoc(docId, {
            "success" : function(data) {
                for (i in data.fileset){
                    if ( i != "files"){
                        // If it isnt the files node, send it to the tree
                        if (typeof(data.fileset[i]) == "object"){
                            // push object to tree
                            acdc.buildTreeBranch(acdc.filesetNode, data.fileset[i]);
                        } else {
                            // wrap value in object & push to tree
                            var dataObj = {};
                            dataObj[i] = data.fileset[i]; 
                            acdc.buildTreeBranch(acdc.filesetNode, dataObj);
                        };
                    };
                }; 
                for (f in data.fileset.files){
                     // files go into the dataTable, not the tree
                     var fileData = data.fileset.files[f];
                     var locations = "<ul>"
                     for (l in fileData.locations){
                         var locString = "<li>" + fileData.locations[l] + "</li>";
                         locations += locString;
                     }
                     locations += "</ul>";
                     acdc.dataTable.addRow(
                         { "lfn" : fileData["lfn"], 
                           "events" : fileData["events"], 
                           "size" : fileData["size"],
                           "locations" : locations}
                         );
                }
            }
        })
    }
  
    
}