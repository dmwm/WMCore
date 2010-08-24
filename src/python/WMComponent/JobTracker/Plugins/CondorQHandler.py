#!/bin/env python

#The handler class to parse condor_q results

from xml.sax.handler import ContentHandler

class CondorQHandler(ContentHandler):
    """
    _CondorQHandler_

    XML SAX Handler to parse the classads returned by the condor_q -xml command

    """
    def __init__(self):
        ContentHandler.__init__(self)
        self.classads = []
        self.thisClassad = None
        self._CharCache = ""
        self.currentClassad = None
        self.boolean = None

    def startElement(self, name, attrs):
        """
        _startElement_

        Override SAX startElement handler
        """
        if name == "c":
            self.thisClassad = {}
            return
        if name == "a":
            adname = attrs.get("n", None)
            if adname == None:
                return
            self.thisClassad[str(adname)] = None
            self.currentClassad = str(adname)
            return
        if name == "b":
            boolValue = attrs.get("v", None)
            if boolValue == None:
                return
            if boolValue == "t":
                self.boolean = True
            else:
                self.boolean = False
            return
        
        
    def endElement(self, name):
        """
        _endElement_

        Override SAX endElement handler
        """
      
        if name == "c":
            self.classads.append(self.thisClassad)
            self.thisClassad = None
            return
        
        if name == "i":
            self.thisClassad[self.currentClassad] = int(self._CharCache)
            return
        if name == "s":
            self.thisClassad[self.currentClassad] = str(self._CharCache)
            return
        if name == "r":
            self.thisClassad[self.currentClassad] = str(self._CharCache)
            return
        if name == "b":
            if self.boolean != None:
                self.thisClassad[self.currentClassad] = self.boolean
                self.boolean = None
                
        
        self._CharCache = ""
        
    def characters(self, data):
        """
        _characters_

        Accumulate character data from an xml element
        """
        self._CharCache += data.strip()
