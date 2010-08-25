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

__revision__ = "$Id: RESTApi.py,v 1.30 2009/12/22 20:08:45 metson Exp $"
__version__ = "$Revision: 1.30 $"

from WMCore.WebTools.WebAPI import WebAPI
from WMCore.WebTools.Page import Page, exposejson, exposexml, make_rfc_timestamp
from WMCore.WMFactory import WMFactory
from cherrypy import expose, request, response, HTTPError
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
        try:
            data, expires = self.methods['handler']['call'](request.method, \
                                                            args, kwargs)
            return self.formatResponse(data, expires, \
                                       kwargs.get('return_type', None))
            
        except HTTPError, h:
            response.status = h[0]
            
            return self.formatResponse({'exception': h[0],
                                        'type': 'HTTPError',
                                        'message': h[1]},
                                        format=kwargs.get('return_type', None))
        except Exception, e:
            response.status = 500
            return self.formatResponse({'exception': 500,
                                        'type': type(e), 
                                        'message': e.message},
                                        format=kwargs.get('return_type', None))
    
    def formatResponse(self, data, expires=False, format=None):
        """
        
        data format can be anything API provides, but it will make sense 
        to have either dict format or list of dict format.
        
        """
        
        acchdr = request.headers.elements('Accept')
        if format:
            datatype = format 
        elif len(acchdr) == 1 and '*/*' == str(acchdr[0]):
            datatype = '*/*'
        else:
            datatype = accept(self.supporttypes)

        if expires:
            response.headers['Expires'] = make_rfc_timestamp(expires)
        else:
            #TODO: pick up the default expires from config
            response.headers['Expires'] = make_rfc_timestamp(5*60)
            
        data = self.formatter.format(data, datatype)
        response.headers['ETag'] = data.__str__().__hash__()
        response.headers['Content-Type'] = datatype
        response.headers['Content-Length'] = len(data)
        return data
