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

__revision__ = "$Id: RESTApi.py,v 1.23 2009/09/22 16:40:08 metson Exp $"
__version__ = "$Revision: 1.23 $"

from WMCore.WebTools.WebAPI import WebAPI
from WMCore.WebTools.Page import Page, exposejson, exposexml, make_rfc_timestamp
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
        self.supporttypes = self.formatter.supporttypes.keys()
        
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
            
            return self.templatepage('RESTAPI', model = self.model, 
                                 types = types,
                                 title = self.config.title,
                                 description = self.config.description)
    
        data = self.methods['handler']['call'](request.method, args, kwargs)
        
        if 'expire' in data.keys():
            response.headers['Expires'] = make_rfc_timestamp(data['expire'])
        else:
            response.headers['Expires'] = make_rfc_timestamp(5*60)

        return self.formatResponse(data)
    
    def formatResponse(self, data):
        acchdr = request.headers.elements('Accept')
        if len(acchdr) == 1 and '*/*' == str(acchdr[0]):
            datatype = '*/*'
        else:
            datatype = accept(self.supporttypes)

        data = self.formatter.format(data, datatype)
        response.headers['ETag'] = data.__str__().__hash__()
        response.headers['Content-Type'] = datatype
        response.headers['Content-Length'] = len(data)
        return data
