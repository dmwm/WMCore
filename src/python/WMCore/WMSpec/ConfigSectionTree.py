#!/usr/bin/env python
# pylint: disable=E1101, W0212
"""
_ConfigSectionTree_

Extension for a normal ConfigSection to provide a Tree structure
of ConfigSections

"""



import types

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

def listChildNodes(topNode):
    """
    _listChildNodes_

    ListNodes but without including the top node
    """
    result = []
    for child in topNode.tree.childNames:
        result.extend(listNodes(getattr(topNode.tree.children, child)))
    return result

def listFirstGenChildNodes(topNode):
    """
    _listFirstGenChildNodes_

    Return a list of the first generator child nodes.
    """
    return topNode.tree.childNames

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
        raise RuntimeError(msg)

    setattr(currentNode.tree.children, newName, newNode)
    currentNode.tree.childNames.append(newName)
    newNode.tree.parent = nodeName(currentNode)
    return

def addTopNode(currentNode, newNode):
    """
    _addTopNode_

    Add a child node to the current node provided but insert it
    at the head of the childNames list.
    """
    newName = nodeName(newNode)
    allNames = allNodeNames(currentNode)

    if newName in allNames:
        msg = "Duplicate Node Name %s already exists in tree\n" % newName
        msg += "%s\n" % allNames
        raise RuntimeError(msg)

    setattr(currentNode.tree.children, newName, newNode)
    currentNode.tree.childNames.insert(0, newName)
    newNode.tree.parent = nodeName(currentNode)
    return

def deleteNode(topNode, childName):
    """
    _deleteNode_

    Given a node within the tree, delete the child
    with the given name if it exists
    """
    if hasattr(topNode.tree.children, childName):
        delattr(topNode.tree.children, childName)
        topNode.tree.childNames.remove(childName)

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

def findTop(node):
    """
    _findTop_

    Ignoring tree structure, find the top node that contains the node
    provided.

    Will work for any ConfigSection, not limited to ConfigSectionTree

    """
    if node._internal_parent_ref == None:
        return node
    return findTop(node._internal_parent_ref)


def nodeIterator(node):
    """
    _nodeIterator_

    Generator function that delivers all nodes in order

    """
    for i in listNodes(node):
        yield getNode(node, i)

def nodeChildIterator(node):
    """
    _nodeChildeIterator_

    iterate over all nodes in order, except for the top node passed to this method
    """
    for i in listChildNodes(node):
        yield getNode(node, i)

def firstGenNodeChildIterator(node):
    """
    _firstGenNodeChildIterator_

    Iterator over all the first generation child nodes.
    """
    for i in listFirstGenChildNodes(node):
        yield getNode(node, i)

def format(value):
    """
    _format_

    format a value as python
    keep parameters simple, trust python...
    """
    if type(value) == str:
        value = "\'%s\'" % value
    return str(value)

def formatNative(value):
    """
    _formatNative_

    Like the format function, but allowing passing of ints, floats, etc.
    """

    if type(value) == int:
        return value
    if type(value) == float:
        return value
    if type(value) == list:
        return value
    if type(value) == dict:
        return dict
    else:
        return format(value)

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

    def isTopOfTree(self):
        """
        _isTopOfTree_

        Determine if this section is the top of the tree.
        """
        return self.data._internal_treetop

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

    def addTopNode(self, newNode):
        """
        _addTopNode_

        Add a child node to the current node provided but insert it
        at the head of the childNames list.
        """
        if isinstance(newNode, TreeHelper):
            return addTopNode(self.data, newNode.data)
        return addTopNode(self.data, newNode)

    def deleteNode(self, nodeName):
        """
        _deleteNode

        Delete a child node given its name,
        if it doesn't exists then do nothing
        """
        deleteNode(self.data, nodeName)
        return


    def allNodeNames(self):
        """get list of all known node names in the tree containing this node"""
        return allNodeNames(self.data)

    def findTopNode(self):
        """get ref to the top node in the tree containing this node"""
        return findTopNode(self.data)

    def getTopConfigSection(self):
        """
        _getTopConfigSection_

        Ignore tree structure & fetch the absolute top of the pile
        ConfigSection containing this node

        """
        return findTop(self.data)

    def nodeIterator(self):
        """
        generator for processing all subnodes in execution order
        """
        for i in listNodes(self.data):
            yield getNode(self.data, i)

    def nodeChildIterator(self):
        """
        generator for processing all subnodes in execution order
        """
        for i in listChildNodes(self.data):
            yield getNode(self.data, i)

    def firstGenNodeChildIterator(self):
        """
        _fristGenNodeChildIterator_

        Iterate over all the first generation child nodes.
        """
        for i in listFirstGenChildNodes(self.data):
            yield getNode(self.data, i)

    def pythoniseDict(self, **options):
        """
        convert self into dict of python format strings with
        values in value position

        """
        prefix     = options.get('prefix',   None)
        sections   = options.get('sections',   False)

        if prefix != None:
            myName = "%s.%s" % (prefix, self.data._internal_name)
        else:
            myName = self.data._internal_name

        result = {}

        for attr in self.data._internal_settings:
            if attr in self.data._internal_children:
                if sections:
                    result["%s.section_(\'%s\')" % (myName, attr)] = '_Section_'
                result.update(TreeHelper(getattr(self.data, attr)).pythoniseDict(prefix = myName))
                continue

            #This is potentially dangerous, because it adds lists, dicts.
            result["%s.%s" %(myName, attr)] = formatNative(getattr(self.data, attr))

        return result


    def addValue(self, value):
        """
        _addValue_

        adds an arbitrary value as a dictionary.  Can have multiple values
        """

        if not type(value) == dict:
            raise Exception("TreeHelper.addValue passed a value that was not a dictionary")

        for key in value.keys():
            splitList = key.split('.')
            setResult = value[key]
            if len(splitList) == 1:
                #Then there is only one level, and we put it here
                setattr(self.data, key, setResult)
            else:
                if splitList[0] in self.data.listSections_():
                    #If the section exists, go to it directly
                    helper = TreeHelper(getattr(self.data, splitList[0]))
                    helper.addValue({splitList[1:]:setResult})
                else:
                    #If the section doesn't exist, create it
                    self.data.section_(splitList[0])
                    helper = TreeHelper(getattr(self.data, splitList[0]))
                    helper.addValue({"".join(splitList[1:]):setResult})
        return





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
