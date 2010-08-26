#!/usr/bin/env python
# pylint: disable-msg=W0613
"""
_IMProvDoc_

Dictionary based container for holding a set of IMProvNodes
representing an XML document. Also provides a way to
add an IMProv structure to a ScriptObject

ToDo - Add XPath search abilities to this object in future

"""
__version__ = "$Revision: 1.1 $"
__revision__ = "$Id: IMProvDoc.py,v 1.1 2008/10/08 15:34:12 fvlingen Exp $"

import os

from xml.dom.minidom import Document
#from xml.dom.ext import PrettyPrint
from IMProv.IMProvNode import IMProvNode



def saveIMProvDoc(soRef, mdName):
    """
    save an IMProv instance as part of a script Object
    """
    improv = soRef.GetAttributeValue(mdName, "Object")
    targetDir = soRef.GetPersistentPath()
    targetFile = "%s-IMProvDoc.xml" % mdName
    target = os.path.join(targetDir, targetFile)
    dom = improv.makeDOMDocument()
    handle = open(target, 'w')
    handle.write(dom.toprettyxml())
    handle.close()
    
    mdLine = "%s SaveFile=%s\n" % (
        mdName,
        target
        )
    return mdLine
    
    

def loadIMProvDoc(soRef, mdName, mdLine):
    """
    load an IMProv instance that was saved as a part
    of a script Object
    """
    pass

class IMProvDoc(IMProvNode):
    """
    _IMProvDoc_

    Document element container that acts as a toplevel
    document for a set of IMProvNodes containing data
    
    """
    
    _Schema = ['Object']
    
    def __init__(self, baseNodeName = "IMProvDoc"):
        IMProvNode.__init__(self, baseNodeName)
        
    def makeDOMDocument(self):
        """
        _makeDOMDocument_

        Create a DOM Document from all sub nodes
        """
        doc = Document()
        doc.appendChild(self.makeDOMElement())
        return doc
    

    
    def addToScriptObject(self, soRef, attrName = "IMProvDoc"):
        """
        _addToScriptObject_

        Add this IMProvDoc instance to the script Object
        instance provided

        DEPRECATED: Use IMProv.ScriptObjectUtils.addIMProvDoc instead
        
        """
        typeVal = str(self.__class__.__name__)
        soRef.addItem( attrName, typeVal , Object=self)
        return
