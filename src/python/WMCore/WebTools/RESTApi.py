#!/usr/bin/env python
"""
_RESTApi_

A standard class implementing a REST interface. You should configure the
application to point at this class, with a model and formatter class configured:

active.section_('rest')
active.rest.object = 'WMCore.WebTools.RESTApi'
active.rest.templates = os.path.join(WMCore.WMInit.getWMBASE(), '/src/templates/WMCore/WebTools/')
active.rest.database = 'MySQL connection string'
active.rest.section_('model')
active.rest.model.object = 'RESTModel'
active.rest.model.database = 'MySQL connection string'
active.rest.model.templates = os.path.join(WMCore.WMInit.getWMBASE(), '/src/templates/WMCore/WebTools/')
active.rest.section_('formatter')
active.rest.formatter.object = 'RESTFormatter'
active.rest.formatter.templates = '/templates/WMCore/WebTools/'

"""

import traceback

from cherrypy import expose, request, response, HTTPError
from cherrypy.lib.cptools import accept

from WMCore.WebTools.WebAPI import WebAPI
from WMCore.WMFactory import WMFactory
from WMCore.Lexicon import replaceToSantizeURL

class RESTApi(WebAPI):
    """
    Don't subclass this, use the RESTModel as a base for your application code
    """
    __version__ = 1
    def __init__(self, config = {}):

        self._set_formatter(config)

        self._set_model(config)

        self.__doc__ = self.model.__doc__

        WebAPI.__init__(self, config)
        self.methods.update({'handler':{'args':[],
                                 'call':self.model.handler,
                                 'version': 1}})

        # TODO: implement HEAD & TRACE
        self.supporttypes = list(self.formatter.supporttypes)

    def _set_model(self, config):
        """
        Load the model object, and assign it to the RESTAPI
        """
        factory = WMFactory('webtools_factory')
        self.model = factory.loadObject(config.model.object, config)

    def _set_formatter(self, config):
        """
        Load the formatter object, and assign it to the RESTAPI
        """
        factory = WMFactory('webtools_factory')
        self.formatter = factory.loadObject(config.formatter.object, config)

    @expose
    def index(self, **kwargs):
        """
        Return the auto-generated documentation for the API
        """
        return self._buildResponse()

    @expose
    def default(self, *args, **kwargs):
        """
        Return either the documentation or run the appropriate method.
        """
        return self._buildResponse(args, kwargs)

    def _buildResponse(self, args=[], kwargs={}):
        """
        Set the headers for the response appropriately and format the response
        data (e.g. serialise to XML, JSON, RSS/ATOM) or return the documentation
        if no method is specified.
        """
        args = list(args)
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
            data, expires = self.methods['handler']['call'](request.method,
                                                            args, kwargs)
            return self._formatResponse(data, expires,
                                       kwargs.get('return_type', None))

        except HTTPError as h:
            # If something raises an HTTPError assume it's something that should
            # go to the client
            response.status = h.args[0]
            self.debug('call to %s with args: %s kwargs: %s resulted in %s' % (request.method, args, kwargs, h.args[1]))
            return self._formatResponse({'exception': h.args[0],
                                        'type': 'HTTPError',
                                        'message': h.args[1]}, expires=0,
                                        format=kwargs.get('return_type', None))
        except Exception as e:
            # If something raises a generic exception assume the details are private
            # and should not go to the client
            response.status = 500
            debugMsg = replaceToSantizeURL("""call to %s with args: %s kwargs: %s resulted in %s \n
                                              stack trace: %s""" % (request.method, args,
                                                     kwargs, e.__str__(), traceback.format_exc()))
            self.debug(debugMsg)
            return self._formatResponse({'exception': 500,
                                        'type': e.__class__.__name__,
                                        'message': 'Server Error'}, expires=0,
                                        format=kwargs.get('return_type', None))

    def _formatResponse(self, data, expires, format=None):
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

        return self.formatter.format(data, datatype, expires)
