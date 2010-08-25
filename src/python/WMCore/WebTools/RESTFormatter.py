#!/usr/bin/env python
"""
_DASRESTFormatter_

A basic REST formatter
"""
from WMCore.WebTools.Page import TemplatedPage, exposejson, exposexml, exposeatom

class RESTFormatter(TemplatedPage):
    supporttypes= ['application/xml', 'application/atom+xml',
                             'text/json', 'text/x-json', 'application/json',
                             'text/html','text/plain']
    
    @exposejson
    def json(self, data):
        return data

    @exposexml
    def xml(self, data):
        return data

    @exposeatom
    def atom(self, data):
        return data