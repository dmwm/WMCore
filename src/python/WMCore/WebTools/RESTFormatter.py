#!/usr/bin/env python
"""
_DASRESTFormatter_

A basic REST formatter.

Could add YAML via http://pyyaml.org/
"""
from WMCore.WebTools.Page import TemplatedPage
from WMCore.WebTools.Page import exposejson, exposexml, exposeatom, exposetext
from cherrypy import response, HTTPError, expose

__revision__ = "$Id: RESTFormatter.py,v 1.22 2010/04/26 19:45:27 sryu Exp $"
__version__ = "$Revision: 1.22 $"

class RESTFormatter(TemplatedPage):
    def __init__(self, config):
        TemplatedPage.__init__(self, config)
        self.supporttypes = {'application/xml': self.xml,
                   'application/atom+xml': self.atom,
                   'text/json': self.json, 
                   'text/x-json': self.json, 
                   'application/json': self.json,
                   'text/html': self.to_string,
                   'text/plain': self.to_string,
                   '*/*': self.to_string}
    
    @exposejson
    def json(self, data):
        return data

    @exposexml
    def xml(self, data):
        return data

    @exposeatom
    def atom(self, data):
        return data
    
    @exposetext
    def to_string(self, data):
        return data
    
    def format(self, data, datatype, expires):
        if datatype not in self.supporttypes.keys():
            response.status = 406
            return self.supporttypes['text/plain']({'exception': 406,
                                                'type': 'HTTPError',
            'message': '%s is not supported. Valid accept headers are: %s' %\
                    (datatype, self.supporttypes.keys())})
        
        try:
            return self.supporttypes[datatype](data, expires, datatype)
        except HTTPError, h:
            response.status = h[0]
            return self.supporttypes[datatype]({'exception': h[0],
                                                'type': 'HTTPError',
                                                'message': h[1]})
        except Exception, e:
            response.status = 500
            return self.supporttypes[datatype]({'exception': 500,
                                                'type': e.__class__.__name__,
                                                'message': str(e)})