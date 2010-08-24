#!/usr/bin/env python
"""
_IMProvNode_

Dictionary based node container for constructing
IMProv Documents from
"""
__version__ = "$Revision: 1.1 $"
__revision__ = "$Id: IMProvNode.py,v 1.1 2008/10/08 15:34:14 fvlingen Exp $"


from xml.dom.minidom import Element, Text

from IMProv.IMProvException import IMProvException



class IMProvNode(dict):
    """
    Node in an IMProv Tree for building a
    tree based information container of nodes
    That can be easily converted to and from XML
    
    """

    def __init__(self, name, text = None, **attrs):
        dict.__init__(self)
        self.name = name
        self.attrs = attrs
        self.chardata = str(text)
        self.children = []


    def addNode(self, node):
        """
        _addNode_

        Add a child node to this node

        Args --

        - *node* : Instance of IMProvNode to be added
        as child
        """
        if not isinstance(node, IMProvNode):
            msg = "Value is not an IMProvNode instance"
            raise IMProvException(
                msg, ClassInstance = self,
                Value = node)
        
        self.children.append(node)
        self[node.name] = node
        return

        
    def __setitem__(self, key, value):
        if not self.has_key(key):
            dict.__setitem__(self, key, [])
        self[key].append(value)
        return


    def processNodes(self, callback):
        """
        _processNodes_

        execute the callback provided on every node in the node tree,
        starting with this node. Traversal is recursive tree descent
        much like SAX
        """
        callback(self)
        for node in self.children:
            node.processNodes(callback)
        return

    def improvOperator(self, operator):
        """
        _improvOperator_

        Method to allow an IMProvOperator instance to traverse the node tree
        and provide the appropriate callbacks

        """
        operator.startNode(self.name, self)
        for node in self.children:
            node.improvOperator(operator)
        operator.endNode(self.name, self)
        return
    

    def makeDOMElement(self):
        """
        _makeDOMElement_

        Create a DOM Element representation of self
        and children
        """
        elem = Element(self.name)
        for attr, value in self.attrs.items():
            elem.setAttribute(attr, str(value))

        if self.chardata not in  (None, str(None) ):
            lines = self.chardata.splitlines()
            for line in lines:
                txtElem = self._MakeTextElem(line)
                elem.appendChild(txtElem)

        for child in self.children:
            childElem = child.makeDOMElement()
            elem.appendChild(childElem)
        return elem
        
    def _MakeTextElem(self, textcontent):
        """
        Text Element construction helper since
        python2.2 and python 2.3 use different implementations
        of the text element
        """
        # 2.3 Text class first
        try:
            textElem = Text()
            textElem.data = textcontent
        except TypeError:
            textElem = Text(textcontent)
        return textElem

    def __str__(self):
        """create string rep of self"""
        strg = '<%s' % self.name
        for key, value in self.attrs.items():
            strg += ' %s=\"%s\" ' % (key, value)
        strg += '>\n'
        if self.chardata not in (None, str(None) ):
            lines = self.chardata.splitlines()
            for line in lines:
                strg += "  %s\n" % line
        for value in self.children:
            strgRep = str(value)
            lines = strgRep.splitlines()
            for line in lines:
                strg += "  %s\n" % line
        strg += "</%s>\n" % self.name
        return strg
        
    
