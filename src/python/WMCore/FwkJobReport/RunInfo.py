#!/usr/bin/env python
"""
_RunInfo_

Run container, contains run number and list of lumi sections contained
therin

"""
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

    def __to_json__(self, thunker):
        """
        __to_json__

        Pull all the lumis out of this and return them as a list.  We don't need
        the run number as that is stored in the fwkjr and used as a key to
        access this object.
        """
        lumis = []
        lumis.extend(self)
        return lumis
