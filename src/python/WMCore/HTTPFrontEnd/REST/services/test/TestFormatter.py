#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
#
# Copyright 2008 Cornell University, Ithaca, NY 14853. All rights reserved.
#
# Author:  Valentin Kuznetsov, 2008
#
# This work based on example from CherryPy Essentials book, by Sylvain Hellegouarch
"""
Example of data formatter used by REST service
"""

import os, sys, string, traceback, simplejson, time, types

# Cheetah template modules
from   Cheetah.Template import Template
from   Templates   import *

def genTopHTML(host,url,mastheadUrl,footerUrl,onload=""):
    nameSpace={
               'host'        : host,
               'baseUrl'     : url,
               'mastheadUrl' : mastheadUrl,
               'footerUrl'   : footerUrl,
               'title'       : 'REST service',
               'onload'      : onload
              }
    t = templateTop(searchList=[nameSpace]).respond()
    return str(t)

def genBottomHTML():
    nameSpace = { 'localtime':time.asctime() }
    t = templateBottom(searchList=[nameSpace]).respond()
    return str(t)

noDataXML="""<?xml version='1.0' standalone='yes'?>
<rest>
<NO_DATA_FOUND />
</rest>
"""
class TestFormatter(object):
    def to_xml(self,data):
        if not data:
            return noDataXML
        elif type(data) is not types.StringType:
            return """<?xml version='1.0' standalone='yes'?><rest>%s</rest>"""%data
        elif data.find("xml")!=-1:
            return """<?xml version='1.0' standalone='yes'?><return>%s</return>"""%data
        else:
            return data

    def to_txt(self,data):
        if not data:
           return {}
        try:
            page = "CONVERT data %s into TEXT, need implementation"%data
            return page
        except:
            traceback.print_exc()
            return str(data)

    def to_json(self,data):
        if not data:
           return {}
        return simplejson.dumps(data, sort_keys=True, indent=4)

    def to_html(self,host,url,mastheadUrl,footerUrl,data):
        try:
            page = genTopHTML(host,url,mastheadUrl,footerUrl)
            page+= "<h3>REST services: </h3>"
            page+="HTML form for data='%s'"%data
            page+= genBottomHTML()
        except:
            traceback.print_exc()
            page ="Unsupported accept type: <pre>%s</pre>"%data.replace("<","&lt;").replace(">","&rt;")
        return page

