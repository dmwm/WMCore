#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
"""
Rest Model abstract implementation
"""

__author__ = "Valentin Kuznetsov <vkuznet at gmail dot com>"
__revision__ = "$Id: RESTModel.py,v 1.28 2009/10/05 15:25:15 sryu Exp $"
__version__ = "$Revision: 1.28 $"

from WMCore.WebTools.WebAPI import WebAPI
from cherrypy import response, request
import sys

class RESTModel(WebAPI):
    """
    Rest model class interface. Subclass this method and add methods to 
    self.methods, *do not* modify/override the handler method unless you really
    know what you're doing.
    """
    def __init__(self, config = {}):
        WebAPI.__init__(self, config)
        self.methods = {'GET':{
                               'ping': {'default_data':1234, 
                                        'call':self.ping,
                                        'version': 1,
                                        'args': [],
                                        'validation': [],
                                        'expires': 3600}
                               },
                        'POST':{
                               'echo': {'call':self.echo,
                                        'version': 1,
                                        'args': ['message'],
                                        'validation': []},
                               }
                         }
        
    def ping(self, verb, args, kwargs): 
        """
        Return a simple message
        """
        return 'hello %s' % self.methods[verb][args[0]]['default_data'] 
    
    def echo(self, verb, args, kwargs):
        """
        Echo back the arguments sent to the call 
        """ 
        return {'echo': {'args': args, 'kwargs':kwargs}}
   
    def handler(self, verb, args=[], kwargs={}):
        """
        Call the appropriate method from self.methods for a given VERB. args are
        URI path elements, the first (arg[0]) is the method name, other elements
        (arg[1:]) are positional arguments to the method. kwargs are query string
        parameters (e.g. method?thing1=abc&thing2=def). All args and kwargs are 
        passed to the model method, this is so configuration for a given method 
        can be identified.
        """
        verb = verb.upper()
        if verb in self.methods.keys():
            method = args[0]
            if method in self.methods[verb].keys():
                data = self.methods[verb][method]['call'](*args[1:], **kwargs)
                if 'expires' in self.methods[verb][method].keys():
                    data['expire'] = self.methods[verb][method]['expires']
                return data
            else:
                data = {"message": "Unsupported method for %s: %s" % (verb, method),
                    "args": args,
                    "kwargs": kwargs}
                self.debug(str(data))
                response.status = 405
                return {'exception': data}
        else:
            data = {"message": "Unsupported VERB: %s" % verb,
                    "args": args,
                    "kwargs": kwargs}
            self.debug(str(data))
            response.status = 501
            return {'exception': data}
        
    def sanitise_input(self, args, kwargs):
        """
        Pull out the necessary input from kwargs (by name) and, failing that, 
        pulls out the number required args from args, which assumes the 
        arguments are positional. 
        
        In all but the most basic cases you'll likely want to over-ride this, or
        at least treat its outcome with deep suspicion.
        
        Returns a dictionary.
        """
        method = sys._getframe(1).f_code.co_name
        input = {}
        verb = request.method.upper()
        for a in self.methods[verb][method]['args']:
            if a in kwargs.keys():
                input[a] = kwargs[a]
            else:
                if len(args):
                    input[a] = args.pop(0)
        return self.validate_input(input, verb, method)
    
    def validate_input(self, input, verb, method):
        """
        Apply some checks to the input data. This needs to be over ridden by any
        subclass. You should throw exceptions if the data is invalid. 
        """
        result = {}
        for fnc in self.methods[verb][method].get('validation', []):
            result.update(fnc(input))
        return result