#!/usr/bin/env python
# pylint: disable-msg=W0104
"""
_Run_

container representing a run, and its constituent lumi sections

"""
__revision__ = "$Id: Run.py,v 1.5 2009/08/24 15:01:37 sfoulkes Exp $"
__version__  = "$Revision: 1.5 $"

from WMCore.DataStructs.WMObject import WMObject

class Run(WMObject):
    """
    _Run_

    Run container, is a list of lumi sections

    """
    def __init__(self, runNumber = None, *newLumis):
        WMObject.__init__(self)
        self.run = runNumber
        self.lumis = []
        self.lumis.extend(newLumis)

    def __str__(self):
        return "Run%s:%s" % (self.run, list(self.lumis))


    def __lt__(self, rhs):
        if self.run != rhs.run:
            return self.run < rhs.run
        return list(self.lumis) < list(rhs.lumis)



    def __gt__(self, rhs):
        if self.run != rhs.run:
            return self.run > rhs.run
        return list(self.lumis) > list(rhs.lumis)


    def extend(self, items):
        self.lumis.extend(items)
        return

    def __cmp__(self, rhs):
        if self.run == rhs.run:
            return cmp(list(self.lumis), list(rhs.lumis))
        if self.run > rhs.run:
            return 1
        if self.run < rhs.run:
            return -1

        return cmp(self, rhs)


    def __add__(self, rhs):
        """
        combine two runs
        """
        if self.run != rhs.run:
            msg = "Adding together two different runs"
            msg += "Run %s does not equal Run %s" % (self.run, rhs.run)
            raise RuntimeError, msg
        
        #newRun = Run(self.run, *self)
        #[ newRun.append(x) for x in rhs if x not in newRun ]
        [ self.lumis.append(x) for x in rhs.lumis if x not in self.lumis ]
        
        return self
    def __iter__(self):
        return self.lumis.__iter__()
    
    def __next__(self):
        return self.lumis.__next__()
    def __len__(self):
        return self.lumis.__len__()
    def __getitem__(self,key):
        return self.lumis.__getitem__(key)
    def __setitem__(self,key,value):
        return self.lumis.__setitem__(key,value)
    def __delitem__(self,key):
        return self.lumis.__delitem__(key)

    def __eq__(self, rhs):
        if not isinstance(rhs, Run) :
            return False
        if self.run != rhs.run:
            return False
        return list(self.lumis) == list(rhs.lumis)

    def __ne__(self, rhs):
        return not self.__eq__(rhs)

    def __hash__(self):

        value = self.run.__hash__()
        self.lumis.sort()
        for lumi in self.lumis:
            value += lumi.__hash__()
        return value
