#!/usr/bin/env python
"""
_DASRESTFormatter_

A REST formatter that appends the DAS headers to the result data
"""




from WMCore.WebTools.Page import exposejsonthunker
from WMCore.WebTools.DASRESTFormatter import DASRESTFormatter

class WorkQueueRESTFormatter(DASRESTFormatter):
    def __init__(self, config):
        DASRESTFormatter.__init__(self, config)
        mimes = {'text/json+thunker':self.jsonThunker, 
                 'application/json+thunker':self.jsonThunker}
        self.supporttypes.update(mimes)

    @exposejsonthunker
    def jsonThunker(self, data):
        return data
