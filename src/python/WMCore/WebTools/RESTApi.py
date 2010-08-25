#!/usr/bin/env python
"""
_RESTApi_

A standard class implementing a REST interface. You should configure the 
application to point at this class, with a model and formatter class configured:

active.section_('rest')
active.rest.object = 'WMCore.WebTools.RESTApi'
active.rest.templates = environ['WTBASE'] + '/templates/WMCore/WebTools/'
active.rest.database = 'sqlite:////Users/metson/Documents/Workspace/GenDB/gendb.lite'
active.rest.section_('model')
active.rest.model.object = 'RESTModel'
active.rest.model.database = 'sqlite:////Users/metson/Documents/Workspace/GenDB/gendb.lite'
active.rest.model.templates = environ['WTBASE'] + '/templates/WMCore/WebTools/'
active.rest.section_('formatter')
active.rest.formatter.object = 'RESTFormatter'
active.rest.formatter.templates = '/templates/WMCore/WebTools/'

"""

__revision__ = "$Id: RESTApi.py,v 1.17 2009/08/16 09:33:10 metson Exp $"
__version__ = "$Revision: 1.17 $"

from WMCore.WebTools.WebAPI import WebAPI
from WMCore.WebTools.Page import Page, exposejson, exposexml
from WMCore.WMFactory import WMFactory
from cherrypy import expose, request, response
from cherrypy.lib.cptools import accept
try:
    # Python 2.6
    import json
except:
    # Prior to 2.6 requires simplejson
    import simplejson as json

class RESTApi(WebAPI):
    """
    Don't subclass this, use the RESTModel as a base for your application code
    """
    __version__ = 1
    def __init__(self, config = {}):
        
        #formatterconfig = config.section_('formatter')
        #formatterconfig.application = config.application
        self.set_formatter(config)
        
        #modelconfig = config.section_('model')
        #modelconfig.application = config.application
        self.set_model(config)
        
        self.__doc__ = self.model.__doc__
        
        WebAPI.__init__(self, config)
        self.methods.update({'handler':{'args':[],
                                 'call':self.model.handler,
                                 'version': 1}})
                            
        # TODO: implement HEAD & TRACE
        self.supporttypes  = ['application/xml', 'application/atom+xml',
                             'text/json', 'text/x-json', 'application/json',
                             'text/html','text/plain']
        
    def set_model(self, config):
        factory = WMFactory('webtools_factory')
        self.model = factory.loadObject(config.model.object, config)
        
    def set_formatter(self, config):
        factory = WMFactory('webtools_factory')
        self.formatter = factory.loadObject(config.formatter.object, config)

    @expose
    def index(self, **kwargs):
        """
        Return the auto-generated documentation for the API
        """
        return self.buildResponse()
    
    @expose
    def default(self, *args, **kwargs):
        """
        Return either the documentation or run the appropriate method.
        """
        return self.buildResponse(args, kwargs)
    
    def buildResponse(self, args=[], kwargs={}):
        """
        Set the headers for the response appropriately and format the response 
        data (e.g. serialise to XML, JSON, RSS/ATOM) or return the documentation
        if no method is specified.
        """
        if len(args) == 0 and len(kwargs) == 0:
            self.debug('returning REST documentation')
            types = []
            for m in dir(self.formatter):
                prop = dir(self.formatter.__getattribute__(m))
                if 'exposed' in prop:
                    
                    types.append(m)
            
            return self.templatepage('RESTAPI', methods = self.model.methods, 
                                 types = types,
                                 title = self.config.title,
                                 description = self.config.description)
    
        data = self.methods['handler']['call'](request.method, args, kwargs)
        return self.formatResponse(data)
    
    def formatResponse(self, data):
        datatype = accept(self.supporttypes)
        response.headers['Content-Type'] = datatype
        
        if datatype in ('text/json', 'text/x-json', 'application/json'):
            # Serialise to json
            data = self.formatter.json(data)
        elif datatype == 'application/xml':
            # Serialise to xml
            try:
                data = self.formatter.plist(data)
            except:
                data = self.formatter.xml(data)
        elif datatype == 'application/atom+xml':
            # Serialise to atom
            data = self.formatter.atom(data)
        else:
            # Just assume a string will do...
            data = str(data)
        # TODO: Add other specific content types
        response.headers['Content-Length'] = len(data)
        return data
