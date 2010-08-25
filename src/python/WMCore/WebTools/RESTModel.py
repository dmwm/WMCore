#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
"""
Rest Model abstract implementation
"""

__author__ = "Valentin Kuznetsov <vkuznet at gmail dot com>"
__revision__ = "$Id: RESTModel.py,v 1.7 2009/07/24 13:47:56 metson Exp $"
__version__ = "$Revision: 1.7 $"

from WMCore.WebTools.WebAPI import WebAPI
from cherrypy import response

class RESTModel(WebAPI):
    """
    Rest model class interface. Subclass this method and add methods to 
    self.methods, *do not* modify/override the handler method unless you really
    know what you're doing.
    """
    def __init__(self, config = {}):
        WebAPI.__init__(self, config)
        self.methods = {'GET':{
                               'ping': {'args':[],
                               'call':self.ping,
                               'version': 1}
                               },
                        'POST':{
                               'echo': {'args':[],
                               'call':self.echo,
                               'version': 1},
                               }
                         }
        
    def ping(self, args, kwargs): 
        return 'hello'
    
    def echo(self, args, kwargs): 
        return {'echo': {'args': args, 'kwargs':kwargs}}
   
    def handler(self, verb, args=[], kwargs={}):
        """
        Call the appropriate method from self.methods for a given VERB. args are
        URI path elements, the first (arg[0]) is the method name, other elements
        (arg[1:]) are positional arguments to the method. kwargs are query string
        parameters (e.g. method?thing1=abc&thing2=def).  
        """
        verb = verb.upper()
        if verb in self.methods.keys():
            if args[0] in self.methods[verb].keys():
                data = self.methods[verb][args[0]]['call'](args[1:], kwargs)
                return data 
            else:
                data = {"message": "Unsupported method for %s: %s" % (verb, args[0]),
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