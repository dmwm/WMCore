#!/usr/bin/env python

class JobDefinition(dict):
    """
    _JobDefinition_

    Data object that contains details of content for a single
    job

    """
    def __init__(self):
        dict.__init__(self)
        self.setdefault("SENames", [])
        self.setdefault("Files", [])
        self.setdefault("MaxEvents", None)
        self.setdefault("SkipEvents", None)


    def getOrderedLFNs(self):
        """
        Get list of ordered LFN's
        """
        self['Files'].sort()    #do we want to do this in place?
        return [x.lfn for x in self['Files']]
    
    
    def getOrderedParentLFNs(self):
        """
        Get list of ordered parent lfn's
        """
        result = []
        for file in self['Files']:
            result.extend(file.getParentLFNs())
        result.sort()
        return result