#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
"""
Rest Model abstract implementation
"""

__revision__ = "$Id: RESTModel.py,v 1.39 2009/12/23 21:41:25 metson Exp $"
__version__ = "$Revision: 1.39 $"

from WMCore.WebTools.WebAPI import WebAPI
from cherrypy import response, request, HTTPError
import sys

class RESTModel(WebAPI):
    """
    Rest model class interface. Subclass this method and add methods to 
    self.methods, *do not* modify/override the handler method unless you really
    know what you're doing.
    """
    def __init__(self, config = {}):
        self.version = __version__
        WebAPI.__init__(self, config)
        self.methods = {'GET':{
                               'ping': {'default_data':1234, 
                                        'call':self.ping,
                                        'version': 1,
                                        'args': [],
                                        'expires': 3600,
                                        'validation': []}
                               },
                        'POST':{
                               'echo': {'call':self.echo,
                                        'version': 1,
                                        'args': ['message'],
                                        'validation': []},
                               }
                         }
        
    def ping(self, *args, **kwargs): 
        """
        Return a simple message
        """
        return 'ping' 
    
    def echo(self, *args, **kwargs):
        """
        Echo back the arguments sent to the call 
        """ 
        return {'args': args, 'kwargs': kwargs}
   
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
                    return data, self.methods[verb][method]['expires']
                else:
                    return data, False
            else:
                data = {"message": "Unsupported method for %s: %s" % (verb, method),
                    "args": args,
                    "kwargs": kwargs}
                self.debug(str(data))
                raise HTTPError(405, data)
        else:
            data = {"message": "Unsupported VERB: %s" % verb,
                    "args": args,
                    "kwargs": kwargs}
            self.debug(str(data))
            response.status = 501
            raise HTTPError(501, data)
        
    def addDAO(self, verb, methodKey, daoStr, args=[], validation=[], version=1):
        """
        add dao in self.methods and wrap it with sanitise_input. Assumes that a 
        DAOFactory instance is available from self.
        """
        def function(args, kwargs):
            # store the method name
            method = methodKey
            input = self.sanitise_input(args, kwargs, method)
            # store the dao
            dao = self.daofactory(classname=daoStr)
            return dao.execute(input)
                  
        self.addMethod(verb, methodKey, function, args, validation, version)
        
    def addWrappedMethod(self, verb, methodKey, funcName, args=[], validation=[], version=1):
        """
        add a method handler in self.methods and wrap it with sanitise_input.
        """
        def function(args, kwargs):
            # store the method name
            method = methodKey
            input = self.sanitise_input(args, kwargs, method)
            # store the function
            func = funcName
            return func(input)
                  
        self.addMethod(verb, methodKey, function, args, validation, version)
        
    def addMethod(self, verb, methodKey, function, args=[], validation=[], version=1):
        """
        add a method handler to self.methods self.methods need to be initialize 
        if sub class hasn't done this already.
        """
        if not self.methods.has_key(verb):
            self.methods[verb] = {}
                  
        self.methods[verb][methodKey] = {'args': args,
                                         'call': function,
                                         'validation': validation,
                                         'version': version}

    def sanitise_input(self, args=[], kwargs={}, method = None):
        """
        Pull out the necessary input from kwargs (by name) and, failing that, 
        pulls out the number required args from args, which assumes the 
        arguments are positional. 
        
        In all but the most basic cases you'll likely want to over-ride this, or
        at least treat its outcome with deep suspicion.
        
        Would be nice to loose the method argument and derive it in this method.
        
        Returns a dictionary.
        """
        
        args = list(args)
        input = {}
        verb = request.method.upper()
        for a in self.methods[verb][method]['args']:
            if a in kwargs.keys():
                input[a] = kwargs[a]
                if len(args):
                    args.pop(0)
            else:
                if len(args):
                    input[a] = args.pop(0)
        return self.validate_input(input, verb, method)
    
    def validate_input(self, input, verb, method):
        """
        Apply some checks to the input data. Run all the validation funstions 
        for the given method. Validation functions should raise exceptions if 
        the data doesn't pass the validation (assert's are your friend!). These
        exceptions are caught here and converted into 400 HTTPErrors.  
        """
        validators = self.methods[verb][method].get('validation', [])
        if len(validators) == 0:
            # Do nothing
            return input
        result = {}
        try:
            for fnc in validators:
                result.update(fnc(input))
            return result
        except Exception, e:
            raise HTTPError(400, e.message)