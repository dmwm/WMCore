#!/usr/bin/env python
"""
_RunInfo_

Run container, contains run number and list of lumi sections contained
therin

"""

__version__ = "$Revision: 1.1 $"
__revision__ = "$Id: RunInfo.py,v 1.1 2008/10/08 15:34:16 fvlingen Exp $"
__author__ = "evansde@fnal.gov"

from IMProv.IMProvNode import IMProvNode
from IMProv.IMProvQuery import IMProvQuery


class RunInfo(list):
    """
    _RunInfo_

    Run number & list of lumis

    """
    def __init__(self, run = None, *lumis ):
        list.__init__(self)
        self.run = run
        self.extend(lumis)



    def save(self):
        """save to improv node"""
        result = IMProvNode("Run", None, ID = str(self.run))
        for lumi in self:
            result.addNode(IMProvNode("LumiSection", None, ID = str(lumi)))

        return result


    def load(self, node):
        """load into self from improv node"""

        runQuery = IMProvQuery("/Run[attribute(\"ID\")]")
        runInfo = runQuery(node)
        if len(runInfo) == 0:
            return

        self.run = int(runInfo[-1])


        lumiQuery = IMProvQuery("/Run/LumiSection[attribute(\"ID\")]")
        lumiInfo =  lumiQuery(node)
        self.extend([int(x) for x in lumiInfo])
        return







