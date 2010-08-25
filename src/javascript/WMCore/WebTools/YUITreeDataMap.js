var tree;
var createYUITree = function(divID, data){
    tree = new YAHOO.widget.TreeView(divID);
	var root = tree.getRoot();
    addTextChild(root, data);
	tree.render();	
}

function addTextChild(parent, currentNode){
	if (!currentNode) {
	   return
	} 
    for	(child in currentNode) {
	   var textNode = new YAHOO.widget.TextNode({
		  label: child,
		  expanded: false
	   }, parent)
	   addTextChild(textNode, currentNode.child)
	} 
}