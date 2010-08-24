#!/usr/bin/env python
# pylint: disable-msg=E1101, W0212
"""
_ConfigSectionTree_

Extension for a normal ConfigSection to provide a Tree structure
of ConfigSections

"""
__revision__ = "$Id: ConfigSectionTree.py,v 1.2 2009/02/05 17:10:28 evansde Exp $"
__version__ = "$Revision: 1.2 $"


from WMCore.Configuration import ConfigSection


def nodeName(node):
    """
    _nodeName_

    Util for extracting node name

    """
    return node._internal_name

def nodeParent(node):
    """
    _nodeParent_

    Util for extracting Node Parent reference

    """
    return node._internal_parent_ref

def listNodes(topNode):
    """
    _listNodes_

    Util to return a list of all node names in a NodeData structure
    returns them in execution order

    """
    result = []
    result.append(nodeName(topNode))
    for child in topNode.tree.childNames:
        result.extend(listNodes(getattr(topNode.tree.children, child)))
    return result




def nodeMap(node):
    """
    _nodeMap_

    Generate a map of node name to node data instance
    Note: This will *not* preserve the execution order of the child nodes
    and should not be used to traverse and operate on nodes where order matters

    """
    result = {}
    result[nodeName(node)] = node
    for child in node.tree.childNames:
        result.update(nodeMap(getattr(node.tree.children, child)))
    return result


def findTopNode(node):
    """
    _findTopNode_

    Find the top node of a tree of nodes given an arbitrary node in the tree
    Checks for non None parent, will also stop if the internal_treetop flag
    is set for the parent
    """
    parent = nodeParent(node)
    if parent == None:
        return node
    if getattr(node, "_internal_treetop", False):
        return node

    if getattr(parent, "_internal_treetop", False):
        return parent
    return findTopNode(nodeParent(node))


def allNodeNames(node):
    """
    _allNodeNames_

    Find the top node in the tree and then get a list of all known names

    """
    topNode = findTopNode(node)
    return listNodes(topNode)



def addNode(currentNode, newNode):
    """
    _addNode_

    Add a child node to the current node provided

    """
    newName = nodeName(newNode)
    allNames = allNodeNames(currentNode)

    if newName in allNames:
        msg = "Duplicate Node Name %s already exists in tree\n" % newName
        msg += "%s\n" % allNames
        raise RuntimeError, msg

    setattr(currentNode.tree.children, newName, newNode)
    currentNode.tree.childNames.append(newName)
    newNode.tree.parent = nodeName(currentNode)
    return

def getNode(node, nodeNameToGet):
    """
    _getNode_

    Given a node within the tree, find the node with the name provided &
    return it.
    returns None if not found

    """
    topNode = findTopNode(node)
    mapping = nodeMap(topNode)
    return mapping.get(nodeNameToGet, None)

def nodeIterator(node):
    """
    _nodeIterator_

    Generator function that delivers all nodes in order

    """
    for i in listNodes(node):
        yield getNode(node, i)

class TreeHelper:
    """
    _TreeHelper_

    Convienience wrapper for a ConfigSectionTree that provides
    all the util methods as a wrapper class to avoid method name
    and attribute collisions in the underlying ConfigSection

    """
    def __init__(self, cfgSectTree):
        self.data = cfgSectTree


    def name(self):
        """get name of this node"""
        return nodeName(self.data)

    def setTopOfTree(self):
        """
        flag this node as the top of the tree
        """
        self.data._internal_treetop = True

    def listNodes(self):
        """list this node and all subnodes"""
        return listNodes(self.data)

    def getNode(self, nodeNameToGet):
        """get a node by name from this tree"""
        return getNode(self.data, nodeNameToGet)

    def getNodeWithHelper(self, nodeNameToGet):
        """get a node wrapped in a TreeHelper instance"""
        return TreeHelper(getNode(self.data, nodeNameToGet))

    def addNode(self, newNode):
        """
        add a new Node, newNode can be a ConfigSectionTree or a TreeHelper
        """
        if isinstance(newNode, TreeHelper):
            return addNode(self.data, newNode.data)
        return addNode(self.data, newNode)

    def allNodeNames(self):
        """get list of all known node names in the tree containing this node"""
        return allNodeNames(self.data)

    def findTopNode(self):
        """get ref to the top node in the tree containing this node"""
        return findTopNode(self.data)

    def nodeIterator(self):
        """
        generator for processing all subnodes in execution order
        """
        for i in listNodes(self.data):
            yield getNode(self.data, i)





class ConfigSectionTree(ConfigSection):
    """
    _ConfigSectionTree_

    Node Tree structure that can be embedded into a ConfigSection
    structure

    """

    def __init__(self, name):
        ConfigSection.__init__(self, name)
        self._internal_treetop = False
        self.section_("tree")
        self.tree.section_("children")
        self.tree.childNames = []
        self.tree.parent = None


