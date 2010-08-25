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
        try:
            return self.supporttypes[datatype](data)
        except HTTPError, h:
            cherrypy.response.status = h[0]
            return self.supporttypes[datatype]({'exception': h[0],
                                                'message': h[1]})
        except Exception, e:
            cherrypy.response.status = 500
            return self.supporttypes[datatype]({'exception': 500,
                                                'message': e.message})