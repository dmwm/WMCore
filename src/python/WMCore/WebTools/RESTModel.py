#!/usr/bin/env python
# I'm afraid *args, **kwargs magic is needed here
# pylint: disable-msg=W0142
#-*- coding: ISO-8859-1 -*-
"""
Rest Model abstract implementation
"""
from functools import wraps
from WMCore.Lexicon import check
from WMCore.WebTools.WebAPI import WebAPI
from cherrypy import request, HTTPError
import cherrypy
import traceback

def restexpose(func):
    """
    Decorate a function to clearly mark it as an exposed REST method
    """
    setattr(func, 'restexposed', True)
    setattr(func, 'exposed', False)
    return func

class RESTModel(WebAPI):
    """
    Rest model class interface. Subclass this method and add methods to
    self.methods, *do not* modify/override the handler method unless you really
    know what you're doing.
    """
    def __init__(self, config):
        WebAPI.__init__(self, config)
        self.methods = {}
        self.daofactory = False
        # If not set expire data after 5 mins
        self.defaultExpires = config.default_expires

    def _classifyHTTPError(self, verb, args, kwargs):
        """
        There are a few cases where input will cause an error which needs
        appropriate classification:
            - request uses a globally unsupported verb (raise 501)
            - request is to a non-existant method (raise 404)
            - request is to a valid method, and uses a valid VERB but the
              VERB isn't supported by the method
        """

        method = args[0]

        he = False
        # Make sure the verb and method are well named
        try:
            check("^[A-Z]+$", verb)
            check("^[a-z][A-Za-z0-9]+$", method)
        except:
            he = HTTPError(400, 'bad VERB or method')

        # Checks whether verb is supported, if not return 501 error
        if verb not in self.methods.keys():
            data =  "Unsupported verb: %s" % (verb)
            he = HTTPError(501, data)
        else:
            # We know how to deal with this VERB
            # Do we know the method?
            method_for_verb = method in self.methods[verb].keys()

            if method_for_verb:
                return
            # Is the method supported for a VERB different to the the requests?
            unsupported_verb = False

            other_verbs = self.methods.keys()
            other_verbs.remove(verb)

            for v in other_verbs:
                unsupported_verb = unsupported_verb | (method in self.methods[v].keys())

            # Checks whether method exists but used wrong verb
            if unsupported_verb:
                data =  "Unsupported method for %s: %s" % (verb, method)
                he = HTTPError(405, data)

            # The method doesn't exist
            elif not method_for_verb and not he:
                data = "Method not found for the verb %s: %s" % (verb, method)
                he = HTTPError(404, data)

        if he:
            self.debug(he[1])
            self.debug(traceback.format_exc())
            raise he

    def handler(self, verb, args, kwargs):
        """
        Call the appropriate method from self.methods for a given VERB. args are
        URI path elements, the first (arg[0]) is the method name, other elements
        (arg[1:]) are positional arguments to the method. kwargs are query string
        parameters (e.g. method?thing1=abc&thing2=def). All args and kwargs are
        passed to the model method, this is so configuration for a given method
        can be identified.
        """

        verb = verb.upper()
        if not args:
            args = []
        if not kwargs:
            kwargs = {}
        self._classifyHTTPError(verb, args, kwargs)
        try:
            method = args[0]
            methodCall = self.methods[verb][method]['call']
            # if there is nothing to be processed about the parameter
            # it will just return original parameters
            params, kwargs = self._processParams(args[1:], kwargs)
            if not getattr(methodCall, 'restexposed', False):
                msg = 'Using an implicitly exposed method, '
                msg += 'please use _addDAO, _addMethod or @restexposed around %s' % method
                self.warning(msg)
            data = methodCall(*params, **kwargs)
        # If a type error is raised the data from the client is bad - 400 error
        except TypeError, e:
            error = e.__str__()
            self.debug(error)
            self.debug(traceback.format_exc())
            raise HTTPError(400, error)
        # Don't need to handle other exceptions here - that's done in RESTAPI
        if 'expires' in self.methods[verb][method].keys():
            return data, self.methods[verb][method]['expires']
        else:
            return data, self.defaultExpires

    def _addDAO(self, verb, methodKey, daoStr, args=[], validation=[],
               version=1, daoFactory = None, expires = None,
               secured=False, security_params={}):
        """
        add dao in self.methods and wrap it with _sanitise_input. Assumes that a
        DAOFactory instance is available from self.
        """
        def function(*args, **kwargs):
            """
            Decorator to process the input by a DAO
            """
            # store the method name
            method = methodKey
            input_data = self._sanitise_input(args, kwargs, method)

            # store the dao
            if daoFactory:
                dao = daoFactory(classname=daoStr)
            else:
                assert self.daofactory, "Cannot _addDAO - do not have a daofactory instance"
                # if the code gets to here the dafactory is a daofactory instead of False
                # disable-msg=E1102
                dao = self.daofactory(classname=daoStr)
                # enable-msg=E1102
            # execute the requested input, now a list of keywords
            return dao.execute(**input_data)

        if expires == None:
            expires = self.defaultExpires
        self._addMethod(verb, methodKey, function, args, validation,
                       version, expires, secured, security_params)

    def _addMethod(self, verb, methodKey, function, args=[],
                  validation=[], version=1, expires = None,
                  secured=False, security_params={}):
        """
        Add a method handler to self.methods self.methods, decorate it such that it
        receives sanitised input and is marked as 'restexposed'.
        """

        if not self.methods.has_key(verb):
            self.methods[verb] = {}
        @wraps(function)
        def wrapper(*input_args, **input_kwargs):
            if secured:
                # set up security
                security = cherrypy.tools.secmodv2
                # security_params should be a dict like:
                #{'role':[], 'group':[], 'site':[], 'authzfunc':None}
                security.callable(
                                role=security_params.get('role', []),
                                group=security_params.get('group', []),
                                site=security_params.get('site', []),
                                authzfunc=security_params.get('authzfunc', None)
                                )
            if len(args) != 0:
                # store the method name
                method = methodKey
                input_data = self._sanitise_input(input_args, input_kwargs, method)
                # store the function
                func = function
                # execute the requested input, now a list of keywords
                return func(**input_data)
            else:
                # If the method isn't meant to have arguments the wrapper isn't needed
                # so we need to raise a 400 error
                if (len(input_args) + len(input_kwargs)):
                    raise HTTPError(400, 'Invalid input: Arguments added where none allowed')
                return function()

        funcRef = wrapper

        setattr(funcRef, 'restexposed', True)

        if expires == None:
            expires = self.defaultExpires
        self.methods[verb][methodKey] = {'args': args,
                                         'call': funcRef,
                                         'validation': validation,
                                         'version': version,
                                         'expires': expires}

    def _sanitise_input(self, input_args=[], input_kwargs={}, method = None):
        """
        Pull out the necessary input from kwargs (by name) and, failing that,
        pulls out the number required args from args, which assumes the
        arguments are positional.

        _sanitise_input is called automatically if you use the _addMethod/_addDAO
        convenience functions. If you add your method to the methods dictionary
        by hand you should call _sanitise_input explicitly.

        In all but the most basic cases you'll likely want to over-ride this, or
        at least treat its outcome with deep suspicion.

        TODO: Would be nice to loose the method argument and derive it in this method.

        Returns a dictionary of validated, sanitised input data.
        """
        verb = request.method.upper()

        if len(input_args):
            input_args = list(input_args)
        if (len(input_args) + len(input_kwargs)) > len(self.methods[verb][method]['args']):
            self.debug('%s to %s expects %s argument(s), got %s' % (verb, method, len(self.methods[verb][method]['args']), (len(input_args) + len(input_kwargs))))
            raise HTTPError(400, 'Invalid input: Input arguments failed sanitation.')
        input_data = {}

        for a in self.methods[verb][method]['args']:
            if a in input_kwargs.keys():
                input_data[a] = input_kwargs[a]
                input_kwargs.pop(a)
            else:
                if len(input_args):
                    input_data[a] = input_args.pop(0)
        if input_kwargs:
            raise HTTPError(400, 'Invalid input: Input arguments failed sanitation.')
        self.debug('%s raw data: %s' % (method, {'args': input_args, 'kwargs': input_kwargs}))
        self.debug('%s sanitised input_data: %s' % (method, input_data))
        return self._validate_input(input_data, verb, method)


    def _validate_input(self, input_data, verb, method):
        """
        Apply some checks to the input data. Run all the validation funstions
        for the given method. Validation functions should raise exceptions if
        the data doesn't pass the validation (assert's are your friend!). These
        exceptions are caught here and converted into 400 HTTPErrors.
        """
        validators = self.methods[verb][method].get('validation', [])
        if len(validators) == 0:
            # Do nothing
            return input_data
        result = {}
        # Run the validation functions, these should raise exceptions. If the
        # exception is an HTTPError re-raise it, else raise a 400 HTTPError (bad input).
        for fnc in validators:
            try:
                filteredInput = fnc(input_data)
            except HTTPError, he:
                self.debug(he)
                raise he
            except Exception, e:
                self.debug(e)
                raise HTTPError(400, 'Invalid input: Input data failed validation.')
            result.update(filteredInput)
        return result

    def _processParams(self, args, kwargs):
        """
        If the args and kwargs needs to be pre-processed (encoded, decoded) according
        to the http header values (i.e. content-type) or convert request.body to parameters,
        overwrite this function in child class

        WARNING: use this with caution, args are list of arguements and kwargs a dict of
        argument and value.

        Overwritten function should have the same return signature [], {}
        """
        return args, kwargs
