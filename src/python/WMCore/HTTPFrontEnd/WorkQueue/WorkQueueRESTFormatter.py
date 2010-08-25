#!/usr/bin/env python
"""
_DASRESTFormatter_

A REST formatter that appends the DAS headers to the result data
"""
__revision__ = "$Id: WorkQueueRESTFormatter.py,v 1.2 2010/04/26 20:01:00 sryu Exp $"
__version__ = "$Revision: 1.2 $"


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
