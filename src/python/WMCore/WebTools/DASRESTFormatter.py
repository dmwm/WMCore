#!/usr/bin/env python
"""
_DASRESTFormatter_

A REST formatter that appends the DAS headers to the result data
"""
__revision__ = "$Id: DASRESTFormatter.py,v 1.5 2009/12/27 11:43:10 metson Exp $"
__version__ = "$Revision: 1.5 $"

from WMCore.WebTools.Page import exposedasjson, exposedasxml
from WMCore.WebTools.Page import exposedasplist
from WMCore.WebTools.RESTFormatter import RESTFormatter
from WMCore.WebTools.Page import DEFAULT_EXPIRE

class DASRESTFormatter(RESTFormatter):
    def __init__(self, config):
        RESTFormatter.__init__(self, config)
        self.supporttypes.update({'text/json+das':self.dasjson})

    @exposedasjson
    def dasjson(self, data, expires=DEFAULT_EXPIRE):
        return data

    @exposedasxml
    def xml(self, data, expires=DEFAULT_EXPIRE):
        return data

    @exposedasplist
    def plist(self, data, expires=DEFAULT_EXPIRE):
        return data
