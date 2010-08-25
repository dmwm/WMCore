
// create a groupuser namespace and fill it with tools to generate groupuser information based stuff
var groupuser = {
    
    // reference for the couch db instance
    couchdb : null,
    // reference for the dataTable if used with the default click response
    dataTable : null,
    
    //
    // Use this to provide the couch db API instance that these tools will use 
    //
    setCouchDB: function(couchRef){
        console.log("couchdb ref set...")
        this.couchdb = couchRef;
    },
    
    //
    // This method is used to respond to clicks on the tree nodes
    // In the case that the node has a user and group associated to it, it will use those 
    // values to query couch and display the documents belonging to that owner in the table
    // 
    // Group only does nothing at present, if it isnt a group or user, nothing happens on click
    //
    labelClickResponse: function(node){
        console.log("label click for " + node['label']);
        var userName = node['GU_user_name'];
        var groupName = node['GU_group_name'];
        if (groupName == undefined){
            // not a group or user node
            console.log('node is not a group/user node');
            return;
        }
        if (userName == undefined){
            // its a group node, but cant think of anthing to do with the table...
            console.log('node is a group node');
            return;
        }
        // user node, invoke the couch view
        groupuser.couchdb.view("GroupUser/owner_group_user",{
              startkey : [groupName, userName], endkey : [groupName, userName],
              success : function(data){
                  // clean out existing rows in the display table
                  if(groupuser.dataTable.getRecordSet().getLength() > 0) { 
                      groupuser.dataTable.deleteRows(0, groupuser.dataTable.getRecordSet().getLength()); 
                  }
                  for (i in data.rows){
                      // add new data rows to the table 
                      groupuser.dataTable.addRow({"group": data.rows[i].key[0], "username": data.rows[i].key[1], "document" : data.rows[i]['value']['id']})


                  }
              }
          });
    },
    
 
    //
    // buildUsers function
    //
    // Given a group node, invoke the view to find all the users in that group and
    // add TextNodes for each one to the group node
    buildUsers : function(groupNode){
       var groupName = groupNode['GU_group_name'];
       groupuser.couchdb.view("GroupUser/group_members", 
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
        groupuser.couchdb.view("GroupUser/groups", 
                 {  success: function(data) { 
                    for (i in data.rows) { 
                        var groupName = data.rows[i]['value']
                        var groupId = data.rows[i]['id']
                        // create a menu node in the groups node for each group
                        var groupNode = new YAHOO.widget.MenuNode(groupName, groupsNode, false);
                        groupNode['GU_doc_id'] = groupId;
                        groupNode['GU_group_name'] = groupName;
                        // populate group with users
                        groupuser.buildUsers(groupNode);
                    }     
                 } // end of success function def 
             });
        // install the labelClick response for the tree & draw it
        tree.subscribe("labelClick", groupuser.labelClickResponse);
        tree.draw();
    },
    
    //
    // buildTable - build a YUI DataTable that the default groupuser.labelClickResponse fills
    // Takes the name of the HTML element to construct the table in
    //
    //
    buildTable : function(tableElement){
        var myColumnDefs = [
            {key:"username" , resizeable:true,sortable:true},
            {key:"group",resizeable:true, sortable:true},
            {key:"document", resizable:true, sortable:false}
        ];

        // backend data source for the table is a JavaScript array that starts empty
        var myDataSource = new YAHOO.util.DataSource([]);
        myDataSource.responseType = YAHOO.util.DataSource.TYPE_JSARRAY;
        myDataSource.responseSchema = {
            fields: ["username","group", "document"]
        };

        // the actual table
        var myDataTable = new YAHOO.widget.DataTable(tableElement,
                myColumnDefs, myDataSource, {});
        // set the groupuser.dataTable reference so that the onClick response can manipulate it
        groupuser.dataTable = myDataTable;
    }
    
}