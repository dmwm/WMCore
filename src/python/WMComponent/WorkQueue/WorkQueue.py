#!/usr/bin/env python
# pylint: disable-msg=W0104
"""
_WorkQueue_

container representing work queue

"""
__revision__ = "$Id: WorkQueue.py,v 1.2 2009/05/08 13:59:59 fisk Exp $"
__version__  = "$Revision: 1.2 $"

from WMCore.DataStructs.WMObject import WMObject
import time

class WQElement(WMObject):
    """
    _WQElement_

    WQElement container

    """
    def __init__(self, idnumber = None, blocks = [ ], locations = [ ], priority = 0, 
                 online = 0, njobs = 0):
        WMObject.__init__(self)
        self.idnumber = idnumber 
        self.blocks = blocks
        self.locations = locations
        self.priority = priority
        self.online = online
        self.njobs = njobs
        self.time = time.time()

    def __cmp__(x, y):
        tfactor = x.time
        current = time.time()
        weight = 0.01
        return (x.priority + weight*(current - x.time)) - (y.priority + weight*(current - y.time))
        

class WorkQueue(WMObject):
    def __init__(self):
        self.elements = [ ]

    def AddElement(self, idnumber = None, blocks = [ ],  priority = 0,  njobs = 0):
        locations = [ ] # Should be automated 
        online = 1 # Should be automated 
        x = WQElement(idnumber, blocks, locations, priority, online, njobs)
        self.elements = self.elements + [ x ] 

    def ReorderList(self):
        self.elements.sort()    

    def SetPriority(self, idnumber, newpriority):
        found = 0
        count = 0
        while (len(self.elements) < count or found):
            if (self.elements[count].idnumber == idnumber):
                found=1
                self.elements[count].priority = newpriority
            if (not found):
                print "Element not found nothing changed"
            else:
                self.ReorderList
             
