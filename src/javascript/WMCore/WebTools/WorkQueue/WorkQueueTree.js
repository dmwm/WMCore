var tree
//Function  creates the tree and 
//builds between 3 and 7 children of the root node:
//var tree
var workQueueTree = function(divID) {

    //instantiate the tree:
    tree = new YAHOO.widget.TreeView(divID);

    for (var i = 0; i < 2 ; i++) {
        var tmpNode = new YAHOO.widget.TextNode("Global Queue -" + i, tree.getRoot(), false);
        // tmpNode.collapse();
        // tmpNode.expand();
        // buildRandomTextBranch(tmpNode);
        buildLargeBranch(tmpNode);
    }

   // Expand and collapse happen prior to the actual expand/collapse,
   // and can be used to cancel the operation
   tree.subscribe("expand", function(node) {
          YAHOO.log(node.index + " was expanded", "info", "example");
          // return false; // return false to cancel the expand
       });

   tree.subscribe("collapse", function(node) {
          YAHOO.log(node.index + " was collapsed", "info", "example");
       });

   // Trees with TextNodes will fire an event for when the label is clicked:
   tree.subscribe("labelClick", function(node) {
          YAHOO.log(node.index + " label was clicked", "info", "example");
       });

    //The tree is not created in the DOM until this method is called:
    tree.draw();
}

//function builds 10 children for the node you pass in:
function buildLargeBranch(node) {
    if (node.depth < 5) {
        YAHOO.log("buildRandomTextBranch: " + node.index, "info", "example");
        for ( var i = 0; i < 5; i++ ) {
            new YAHOO.widget.TextNode("Local Queue -" + i, node, false);
        }
    }
};