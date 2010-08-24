#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
"""
Example of data formatter used by REST service
"""

__author__ = "Valentin Kuznetsov <vkuznet at gmail dot com>"
__revision__ = "$Id:"
__version__ = "$Revision:"


import traceback
import simplejson
import time
import types

# Cheetah template modules
#from   Cheetah.Template import Template
from   WMCore.HTTPFrontEnd.REST.Templates   import templateTop
from   WMCore.HTTPFrontEnd.REST.Templates   import templateBottom

def tophtml(url, onload=""):
    """Create header of HTML response"""
    namespace = {
                 'host'        : url,
                 'title'       : 'REST service',
                 'onload'      : onload
                }
    return str( templateTop(searchList=[namespace]).respond() )

def endhtml():
    """Create bottom of HTML response"""
    namespace = { 'localtime':time.asctime() }
    return str ( templateBottom(searchList=[namespace]).respond() )

NODATA = """<?xml version='1.0' standalone='yes'?>
<rest>
<NO_DATA_FOUND />
</rest>
"""
class TestFormatter(object):
    """Simple formatter class. It should format input data according
       to returned MIME type
    """
    def __init__(self):
        self._data = 0 # hold some data

    def to_xml(self, data):
        """This method shows how to convert input data into XML form"""
        # you can do something with data
        self._data = data
        if  not data:
            return NODATA
        elif type(data) is not types.StringType:
            msg  = """<?xml version='1.0' standalone='yes'?>"""
            msg += "<rest>%s</rest>" % data
            return msg
        elif data.find("xml") != -1:
            msg  = """<?xml version='1.0' standalone='yes'?>"""
            msg += "<return>%s</return>" % data
            return msg
        else:
            return data

    def to_txt(self, data):
        """This method shows how to convert input data into TXT form"""
        # you can do something with data
        self._data = data
        if  not data:
            return {}
        try:
            page = "CONVERT data %s into TEXT, need implementation" % data
            return page
        except RuntimeError, err:
            traceback.print_exc()
            page  = err.value
            page += str(data)
            return page

    def to_json(self, data):
        """This method shows how to convert input data into JSON form"""
        # you can do something with data
        self._data = data
        if  not data:
            return {}
        return simplejson.dumps(data, sort_keys=True, indent=4)

    def to_html(self, url, data):
        """This method shows how to convert input data into HTML form"""
        # you can do something with data
        self._data = data
        try:
            page  = tophtml(url)
            page += "<h3>REST services: </h3>"
            page += "HTML form for data='%s'" % data
            page += endhtml()
        except RuntimeError, err:
            traceback.print_exc()
            page  = err.value
            page += "Unsupported accept type: <pre>%s</pre>" % \
                    data.replace("<","&lt;").replace(">","&rt;")
        return page

