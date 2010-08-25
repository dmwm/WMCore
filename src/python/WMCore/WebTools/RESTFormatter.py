#!/usr/bin/env python
"""
_DASRESTFormatter_

A basic REST formatter.

Could add YAML via http://pyyaml.org/
"""
from WMCore.WebTools.Page import TemplatedPage, exposejson, exposexml, exposeatom

class RESTFormatter(TemplatedPage):
    def __init__(self, config):
        TemplatedPage.__init__(self, config)
        self.supporttypes= {'application/xml': self.xml,
                   'application/atom+xml': self.atom,
                   'text/json': self.json, 
                   'text/x-json': self.json, 
                   'application/json': self.json,
                   'text/html': str,
                   'text/plain': str,
                   '*/*': str}
    
    @exposejson
    def json(self, data):
        return data

    @exposexml
    def xml(self, data):
        return data

    @exposeatom
    def atom(self, data):
        return data
    
    def format(self, data, datatype):
        return self.supporttypes[datatype](data)