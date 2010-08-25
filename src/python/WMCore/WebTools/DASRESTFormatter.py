#!/usr/bin/env python
"""
_DASRESTFormatter_

A REST formatter that appends the DAS headers to the result data
"""
__revision__ = "$Id: DASRESTFormatter.py,v 1.6 2010/01/19 15:54:05 valya Exp $"
__version__ = "$Revision: 1.6 $"

from WMCore.WebTools.Page import exposedasjson, exposedasxml
from WMCore.WebTools.Page import exposedasplist
from WMCore.WebTools.RESTFormatter import RESTFormatter
from WMCore.WebTools.Page import DEFAULT_EXPIRE

class DASRESTFormatter(RESTFormatter):
    def __init__(self, config):
        RESTFormatter.__init__(self, config)
        mimes = {'text/json+das':self.dasjson, 'application/xml+das':self.xml,
                 'application/plist':self.plist}
        self.supporttypes.update(mimes)

    @exposedasjson
    def dasjson(self, data, expires=DEFAULT_EXPIRE):
        return data

    @exposedasxml
    def xml(self, data, expires=DEFAULT_EXPIRE):
        return data

    @exposedasplist
    def plist(self, data, expires=DEFAULT_EXPIRE):
        return data
