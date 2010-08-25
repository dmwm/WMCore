#!/usr/bin/env python
"""
_DASRESTFormatter_

A REST formatter that appends the DAS headers to the result data
"""
__revision__ = "$Id: WorkQueueRESTFormatter.py,v 1.1 2010/02/01 17:56:42 sryu Exp $"
__version__ = "$Revision: 1.1 $"


from WMCore.WebTools.Page import exposejsonthunker
from WMCore.WebTools.DASRESTFormatter import DASRESTFormatter
from WMCore.WebTools.Page import DEFAULT_EXPIRE

class WorkQueueRESTFormatter(DASRESTFormatter):
    def __init__(self, config):
        DASRESTFormatter.__init__(self, config)
        mimes = {'text/json+thunker':self.jsonThunker, 'application/json+thunker':self.jsonThunker}
        self.supporttypes.update(mimes)

    @exposejsonthunker
    def jsonThunker(self, data, expires=DEFAULT_EXPIRE):
        return data
