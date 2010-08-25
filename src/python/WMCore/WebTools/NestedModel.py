'''
Created on 5 Jan 2010

@author: metson
'''
from WMCore.WebTools.RESTModel import RESTModel
from cherrypy import response, request, HTTPError

class NestedModel(RESTModel):
    """
    A RESTModel that can also be used as a container for other RESTModels 
    """
    def __init__(self, config = {}):
        RESTModel.__init__(self, config)
        
        self.methods = {'GET':{
                               'foo': {
                                        'default':{'default_data':1234, 
                                                   'call':self.foo,
                                                   'version': 1,
                                                   'args': ['message'],
                                                   'expires': 3600,
                                                   'validation': []},
                                        'ping':{'default_data':1234, 
                                               'call':self.ping,
                                               'version': 1,
                                               'args': [],
                                               'expires': 3600,
                                               'validation': []}}
                               }
                        }
        
    def foo(self, message = None): 
        """
        Return a different simple message
        """
        if message:
            return 'foo ' + message
        else:
            return 'foo'
     
    def handler(self, verb, args=[], kwargs={}):
        """
        Call the appropriate method from self.methods for a given VERB. kwargs 
        are query string parameters (e.g. method?thing1=abc&thing2=def). 

        args are URI path elements, the first (arg[0]) is the base method name, 
        other elements (arg[1:]) are either positional arguments to the base 
        method or further components of the URI path, pointing at other methods.
        
        The default method should have a small number of arguments (preferably 
        none). Any arguments passed to method beyond that number are checked to 
        be path components, e.g. other method names. 
        """
        verb = verb.upper()
        print '################################'
        print 'NestedMethod handler'
        self.classifyHTTPError(verb, args, kwargs)
        args = list(args)
        basemethnom = args[0]
        basemethod = self.methods[verb][basemethnom]
        children = self.methods[verb][basemethnom].keys()
        method = children.pop(children.index('default'))
        print 'basemethod', basemethnom
        print 'children', children
        try:
            print 'args', args
            print 'args[1:]', args[1:]
            
            # is there a method in the keywords?
            for a in kwargs.keys():
                print 'kwargs a', a
                if a in children:
                    method = a
                    if not len(kwargs[a]): 
                        kwargs.pop(a)
            # is there a method in the positional args?
            for a in args[1:]:
                print 'args a', a
                if a in children:
                    method = args.pop(args.index(a))
            
            print 'method', method        
            data = basemethod[method]['call'](*args[1:], **kwargs)
            print '################################'
        # in case sanitise_input is not called with in the method, if args doesn't
        # match throws the 400 error
        except TypeError, e:
            raise HTTPError(400, str(e))
       
        if 'expires' in basemethod[method].keys():
            return data, basemethod[method]['expires']
        else:
            return data, False