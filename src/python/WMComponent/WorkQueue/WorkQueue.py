#!/usr/bin/env python
# pylint: disable-msg=W0104
"""
_WorkQueue_

container representing work queue

"""
__revision__ = "$Id: WorkQueue.py,v 1.1 2009/05/08 10:49:06 fisk Exp $"
__version__  = "$Revision: 1.1 $"

from WMCore.DataStructs.WMObject import WMObject


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
        self.next = "Null"

class WorkQueue(WMObject):
    def __init__(self):
        self.elements = [ ]
 
       


