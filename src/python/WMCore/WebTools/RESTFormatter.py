#!/usr/bin/env python
"""
_DASRESTFormatter_

A basic REST formatter.

Could add YAML via http://pyyaml.org/
"""
from WMCore.WebTools.Page import TemplatedPage
from WMCore.WebTools.Page import exposejson, exposexml, exposeatom
from WMCore.WebTools.Page import DEFAULT_EXPIRE
from cherrypy import response, HTTPError



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
    def json(self, data, expires=DEFAULT_EXPIRE):
        return data

    @exposexml
    def xml(self, data, expires=DEFAULT_EXPIRE):
        return data

    @exposeatom
    def atom(self, data, expires=DEFAULT_EXPIRE):
        return data
    
    def format(self, data, datatype, expires=DEFAULT_EXPIRE):
        try:
            return self.supporttypes[datatype](data, expires=expires)
        except HTTPError, h:
            response.status = h[0]
            return self.supporttypes[datatype]({'exception': h[0],
                                                'type': 'HTTPError',
                                                'message': h[1]})
        except Exception, e:
            response.status = 500
            return self.supporttypes[datatype]({'exception': 500,
                                                'type': type(e),
                                                'message': e.message})