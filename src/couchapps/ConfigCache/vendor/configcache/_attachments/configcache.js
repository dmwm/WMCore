
// create a namespace var for the configcache tools

var configcache = {
    
    // reference for the couch db instance
    couchdb : null,
    
    // dataTable  reference to the main dataTable
    dataTable : null,
    // detailsPain - reference to the details pane
    detailsPane : null, 
    
    logthis : function (log_txt) {
        if (window.console != undefined) {
            configcache.logthis(log_txt);
        }
    },
    
    //
    // Use this to provide the couch db API instance that these tools will use 
    //
    setCouchDB: function(couchRef){
        configcache.logthis("couchdb ref set...")
        this.couchdb = couchRef;
    },
    
    
    //
    // function to draw the detailsPane as a YUI Pannel
    //
    drawDetailsPane: function(element){
        configcache.logthis("drawDetailsPane called...");
        configcache.detailsPane = new YAHOO.widget.Panel(element, 
        	{ 
        	    height:"300px",
        	    width:"650px",
        	    close:false,  
        	    visible:true,  
        	    draggable:false
        	} 
        ); 
        configcache.detailsPane.setHeader('Configuration Details');
        configcache.detailsPane.setBody('Select a Configuration from the table above to see details');
        configcache.detailsPane.setFooter('Tweet This Configuration and look really cool...');
        configcache.detailsPane.render();
    },
    
    
    //
    // label click in the tree populates the data table
    //
    labelClickResponse: function(node){
        configcache.logthis("label click for " + node['label']);
        var userName = node['GU_user_name'];
        var groupName = node['GU_group_name'];
        if (groupName == undefined){
            // not a group or user node
            configcache.logthis('node is not a group/user node');
            return;
        }
        if (userName == undefined){
            // its a group node, but cant think of anthing to do with the table...
            configcache.logthis('node is a group node');
            return;
        }
        // user node, invoke the couch view
        configcache.couchdb.view("ConfigCache/config_by_owner",{
              startkey : [groupName, userName], endkey : [groupName, userName],
              success : function(data){
                  // clean out existing rows in the display table
                  if(configcache.dataTable.getRecordSet().getLength() > 0) { 
                      configcache.dataTable.deleteRows(0, configcache.dataTable.getRecordSet().getLength()); 
                  }
                  for (i in data.rows){
                      // add new data rows to the table 
                      configcache.dataTable.addRow({"group": data.rows[i].key[0], "username": data.rows[i].key[1], "document" : data.rows[i]['value']['config_doc'], "label" : data.rows[i]['value']['config_label']})

                  }
              }
          });
    },
    
    // cell click response
    cellClick: function(arg1){
        var record = configcache.dataTable.getRecord(arg1['target']);
        var recordData = record.getData();
        var documentId = recordData['document'];
        configcache.couchdb.openDoc(documentId, { "success" : function(data){
            var configUrl = configcache.couchdb.uri + "/" + documentId + "/" + "configFile";
            var iframeContent = "<iframe width=\"100%\" height=\"100%\" src=\"" + configUrl + "\"> <p> No iframe support?</p></iframe>";
            configcache.detailsPane.setBody(iframeContent);
        }
        })
        
    },
    
    //
    // draw the table 
    //
    buildTable : function(tableElement){
        var myColumnDefs = [
            {key:"username" , resizeable:true,sortable:true, width:150},
            {key:"group",resizeable:true, sortable:true,width:150},
            {key:"label",resizeable:true, sortable:true,width:150},
            {key:"document", resizable:true, sortable:false,width:150}
        ];

        // backend data source for the table is a JavaScript array that starts empty
        var myDataSource = new YAHOO.util.DataSource([]);
        myDataSource.responseType = YAHOO.util.DataSource.TYPE_JSARRAY;
        myDataSource.responseSchema = {
            fields: ["username","group","label", "document"]
        };

        // the actual table
        var myDataTable = new YAHOO.widget.DataTable(tableElement,
                myColumnDefs, myDataSource, {"width" : 650});
        myDataTable.subscribe("rowClickEvent", configcache.cellClick);
        // set the groupuser.dataTable reference so that the onClick response can manipulate it
        configcache.dataTable = myDataTable;
    },
    
    //
    // buildTree - function to build the tree in the element provided
    // treeElement should be the name of an empty div in the page using its id attribute
    // Eg: buildTree("treeElem") will expect a <div id="treeElem"></div> in the page
    // and put the tree in that
    buildTree : function(treeElement){
        tree = new YAHOO.widget.TreeView(treeElement); 
        // build a top level groups node
        var groupsNode = new YAHOO.widget.MenuNode("Groups", tree.getRoot(), false);
        // call couch view to list the groups
        configcache.couchdb.view("GroupUser/groups", 
                 {  success: function(data) { 
                    for (i in data.rows) { 
                        var groupName = data.rows[i]['value']
                        var groupId = data.rows[i]['id']
                        // create a menu node in the groups node for each group
                        var groupNode = new YAHOO.widget.MenuNode(groupName, groupsNode, false);
                        groupNode['GU_doc_id'] = groupId;
                        groupNode['GU_group_name'] = groupName;
                        // populate group with users
                        configcache.buildUsers(groupNode);
                    }     
                 } // end of success function def 
             });
        // install the labelClick response for the tree & draw it
        tree.subscribe("labelClick", configcache.labelClickResponse);
        tree.draw();
    },
    
    //
    // buildUsers function
    //
    // Given a group node, invoke the view to find all the users in that group and
    // add TextNodes for each one to the group node
    buildUsers : function(groupNode){
       var groupName = groupNode['GU_group_name'];
       configcache.couchdb.view("GroupUser/group_members", 
                {  startkey: [groupName], endkey: [groupName],
                   success: function(data) { 
                   for (i in data.rows) { 
                       userName = data.rows[i]['value']['user']
                       userDoc  = data.rows[i]['value']['id']
                       var userNode = new YAHOO.widget.TextNode(userName, groupNode, false);
                       userNode['GU_doc_id'] = userDoc;
                       userNode['GU_user_name'] = userName;
                       userNode['GU_group_name'] = groupName;
                   }     
                   
                } // end of success function def
            });
    }
    
} //end namespace var