#!/usr/bin/env python
"""
_DASRESTFormatter_

A REST formatter that appends the DAS headers to the result data
"""
__revision__ = "$Id: DASRESTFormatter.py,v 1.7 2010/02/08 12:25:12 metson Exp $"
__version__ = "$Revision: 1.7 $"

# expires is used by the decorator to set the expires header
# pylint: disable-msg=W0613
# I want dasjson and plist to be methods instead of functions
# pylint: disable-msg=R0201

from WMCore.WebTools.Page import exposedasjson, exposedasxml
from WMCore.WebTools.Page import exposedasplist
from WMCore.WebTools.RESTFormatter import RESTFormatter
from WMCore.WebTools.Page import DEFAULT_EXPIRE


class DASRESTFormatter(RESTFormatter):
    """
    A REST formatter that appends the DAS headers to the result data
    """
    def __init__(self, config):
        "Initialise the formatter and set the mime types it supports"
        RESTFormatter.__init__(self, config)
        mimes = {'text/json+das':self.dasjson, 'application/xml+das':self.xml,
                 'application/plist':self.plist}
        self.supporttypes.update(mimes)

    @exposedasjson
    def dasjson(self, data, expires=DEFAULT_EXPIRE):
        "Return DAS compliant json"
        return data
    
    @exposedasxml
    def xml(self, data, expires=DEFAULT_EXPIRE):
        "Return DAS compliant xml"
        return data
    
    @exposedasplist
    def plist(self, data, expires=DEFAULT_EXPIRE):
        "Return DAS compliant plist xml"
        return data