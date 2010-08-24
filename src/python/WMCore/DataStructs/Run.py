#!/usr/bin/env python
# pylint: disable-msg=W0104
"""
_Run_

container representing a run, and its constituent lumi sections

"""
__revision__ = "$Id: Run.py,v 1.1 2008/09/21 12:06:45 evansde Exp $"
__version__  = "$Revision: 1.1 $"

from WMCore.DataStructs.WMObject import WMObject

class Run(WMObject, list):
    """
    _Run_

    Run container, is a list of lumi sections

    """
    def __init__(self, runNumber = None, *lumis):
        WMObject.__init__(self)
        list.__init__(self)
        self.run = runNumber
        self.extend(lumis)

    def __str__(self):
        return "Run%s:%s" % (self.run, list(self))


    def __lt__(self, rhs):
        if self.run != rhs.run:
            return self.run < rhs.run
        return list(self) < list(rhs)



    def __gt__(self, rhs):
        if self.run != rhs.run:
            return self.run > rhs.run
        return list(self) > list(rhs)


    def __cmp__(self, rhs):
        if self.run == rhs.run:
            return cmp(list(self), list(rhs))
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

        [ self.append(x) for x in rhs if x not in self ]
        return self



    def __eq__(self, rhs):
        if self.run != rhs.run:
            return False
        return list(self) == list(rhs)

    def __ne__(self, rhs):
        return not self.__eq__(rhs)

    def __hash__(self):
        value = self.run.__hash__()
        self.sort()
        for lumi in self:
            value += lumi.__hash__()
        return value
