#!/usr/bin/env python
"""
_DASRESTFormatter_

A REST formatter that appends the DAS headers to the result data
"""

from WMCore.WebTools.Page import exposedasjson, exposedasxml
from WMCore.WebTools.Page import exposedasplist
from WMCore.WebTools.RESTFormatter import RESTFormatter

class DASRESTFormatter(RESTFormatter):
    @exposedasjson
    def json(self, data):
        return data

    @exposedasxml
    def xml(self, data):
        return data

    @exposedasplist
    def plist(self, data):
        return data
