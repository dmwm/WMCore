#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
"""
Rest Model abstract implementation

TODO: Decide on refactoring this into a sub class of a VERB implementation...
"""

__revision__ = "$Id: RESTModel.py,v 1.61 2010/04/26 21:16:09 sryu Exp $"
__version__ = "$Revision: 1.61 $"

from WMCore.WebTools.WebAPI import WebAPI
from cherrypy import response, request, HTTPError
import sys
import traceback

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
        
        self.defaultExpires = config.default_expires
        
    def ping(self): 
        """
        Return a simple message
        """
        return 'ping' 
    
    def echo(self, *args, **kwargs):
        """
        Echo back the arguments sent to the call 
        """ 
        return {'args': args, 'kwargs': kwargs}
    
    def classifyHTTPError(self, verb, args, kwargs):
        
        # Checks whether verb is supported, if not return 501 error
        if verb not in self.methods.keys():
            data =  "Unsupported VERB: %s, args: %s, kwargs: %s" % (verb, args, kwargs)
            self.debug(data)
            raise HTTPError(501, data)
        method = args[0]
        
        flag = True
        for v in self.methods.keys():
            if method in self.methods[v].keys():
                flag = False
                break
        if flag:         
            data =  "Unsupported method for %s: %s args: %s, kwargs: %s" \
                     % (verb, method, args, kwargs)
            self.debug(data)
            raise HTTPError(404, data)
        
        # Checks whether method exist but used wrong verb, if not 
        # send 404 error 
        if method not in self.methods[verb].keys():
            data = "Unsupported method for the verb %s: %s args: %s, kwargs: %s" \
                     % (verb, method, args, kwargs)
            self.debug(data)
            raise HTTPError(405, data)
            
    def handler(self, verb, args=[], kwargs={}):
        """
        Call the appropriate method from self.methods for a given VERB. args are
        URI path elements, the first (arg[0]) is the method name, other elements
        (arg[1:]) are positional arguments to the method. kwargs are query string
        parameters (e.g. method?thing1=abc&thing2=def). All args and kwargs are 
        passed to the model method, this is so configuration for a given method 
        can be identified.
        
        TODO: need to refactor exception handling
        """
        verb = verb.upper()
        
        self.classifyHTTPError(verb, args, kwargs)
        try:
            method = args[0]
            # if there is nothing to be processed about the parameter
            # it will just return original parameters
            params, kwargs = self.processParams(args[1:], kwargs)
            data = self.methods[verb][method]['call'](*params, **kwargs)
        # in case sanitise_input is not called with in the method, if args doesn't
        # match throws the 400 error
        except TypeError, e:
            error = e.__str__()
            self.debug(error)
            self.debug(traceback.format_exc())
            raise HTTPError(400, error)
        except HTTPError, he:
            error = he.__str__()
            self.debug(error)
            self.debug(traceback.format_exc())
            raise 
        #other exceptions report 500 error
        except Exception, e:
            error = e.__str__()
            self.debug(error)
            self.debug(traceback.format_exc())
            raise 
       
        if 'expires' in self.methods[verb][method].keys():
            return data, self.methods[verb][method]['expires']
        else:
            return data, self.defaultExpires
                
    def addDAO(self, verb, methodKey, daoStr, args=[], 
               validation=[], version=1, daoFactory = None,
               expires = None):
        """
        add dao in self.methods and wrap it with sanitise_input. Assumes that a 
        DAOFactory instance is available from self.
        """
        def function(*args, **kwargs):
            # store the method name
            method = methodKey
            input = self.sanitise_input(args, kwargs, method)
            # store the dao
            if daoFactory:
                dao = daoFactory(classname=daoStr)
            else:
                dao = self.daofactory(classname=daoStr)
            # execute the requested input, now a list of keywords
            return dao.execute(**input)
        
        if expires == None:
            expires = self.defaultExpires          
        self.addMethod(verb, methodKey, function, args, validation, 
                       version, expires)
        
    def addMethod(self, verb, methodKey, function, args=[], 
                  validation=[], version=1, expires = None):
        """
        add a method handler to self.methods self.methods need to be initialize 
        if sub class hasn't done this already.
        """
        if not self.methods.has_key(verb):
            self.methods[verb] = {}
        
        def wrapper(*args, **kwargs):
            # store the method name
            method = methodKey
            input = self.sanitise_input(args, kwargs, method)
            # store the function
            func = function
            # execute the requested input, now a list of keywords
            return func(**input)
        
        if len(validation) != 0:
            funcRef = wrapper
        else:
            funcRef = function
        
        if expires == None:
            expires = self.defaultExpires    
        self.methods[verb][methodKey] = {'args': args,
                                         'call': funcRef,
                                         'validation': validation,
                                         'version': version,
                                         'expires': expires}

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
        if len(args):
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
        # Run the validation functions, these should raise exceptions. If the  
        # exception is an HTTPError re-raise it, else raise a 400 HTTPError.
        for fnc in validators:
            try:
                filteredInput = fnc(input)
            # if validate function raises HTTPError just forward the error
            except HTTPError, he:
                raise he
            # For other errors wrap into 400 error, assuming the validation functions
            # are tested and working properly.
            except Exception, e:
                raise HTTPError(400, str(e))
            result.update(filteredInput)
        return result

    def processParams(self, args, kwargs):
        """
        If the args and kwargs needs to be processed (encoded, decoded) according to the 
        http header values (i.e. content-type) or convert request.body to parameters, 
        overwrite this function in child class
        
        Warning: use this as caution,  args are list of arguement and kwargs dict of argument and value.
        overwritten function should return same type of tuple [], {}  
        """
        return args, kwargs
