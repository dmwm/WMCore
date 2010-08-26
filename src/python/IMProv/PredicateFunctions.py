#!/usr/bin/env python
"""
_PredicateFunctions_

Implementations of Predicate functions that evaluate themselves on
a node instance.

Predicate Functions act on a node with any extra arguments provided
to return some value based on the nodes content.
The first argument accepted by a Predicate Function must be
an IMProvNode instance.

"""
__revision__ = "$Id: PredicateFunctions.py,v 1.1 2008/10/08 15:34:15 fvlingen Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "evansde@fnal.gov"


def registerPredicateFunction(funcName, funcRef):
    """
    _registerPredicateFunction_

    Register a predicate function with the name provided, if the
    name already exists, then raise an Error.
    """
    test = PredicateFunctions._PredicateMap.get(funcName, None)
    if test != None:
        msg = "Registered Duplicate Predicate Function: %s" % funcName
        raise RuntimeError, msg
    PredicateFunctions._PredicateMap[funcName] = funcRef
    return



def getPredicateFunction(funcName):
    """
    _getPredicateFunction_

    return a reference to the Predicate function requested by name
    """
    return PredicateFunctions._PredicateMap.get(funcName, None)


class PredicateFunctions:
    """
    _PredicateFunctions_

    Namespace used to store predicate functions by name
    """
    _PredicateMap = {}
    def __init__(self):
        msg = "PredicateFunctions is a namespace that should not be instantiated"
        raise RuntimeError, msg

    getPredicateFunction = staticmethod(getPredicateFunction)
    registerPredicateFunction = staticmethod(registerPredicateFunction)
    
    


def attribute(improvNode, *args):
    """
    _attribute_

    return the attribute value with the name provided, or None if it
    does not exist
    """
    attrName = args[0]
    return improvNode.attrs.get(attrName, None)


def text(improvNode, *args):
    """
    _text_
    
    return the text attribute of an improvNode as a string
    """
    return improvNode.chardata


def hasAttribute(improvNode, *args):
    """
    _hasAttribute_

    return True is the improvNode has an attribute with
    with the required name
    """
    attrName = args[0]
    return improvNode.attrs.has_key(attrName)

def hasChild(improvNode, *args):
    """
    _hasChild_

    return True if a child of the node has the name
    childName
    """
    childName = args[0]
    for child in improvNode.children:
        if child.name == childName:
            return True
    return False

def hasChildren(improvNode, *args):
    """
    _hasChildren_

    return True if node has children
    """
    if len(improvNode.children) > 0:
        return True
    return False


PredicateFunctions.registerPredicateFunction("attribute", attribute)
PredicateFunctions.registerPredicateFunction("text", text)
PredicateFunctions.registerPredicateFunction("hasAttribute", hasAttribute)
PredicateFunctions.registerPredicateFunction("hasChild", hasChild)
PredicateFunctions.registerPredicateFunction("hasChildren", hasChildren)
