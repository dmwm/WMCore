#!/usr/bin/env python
"""
This is an example of how to implement simple REST service.
We define Service class who hold the REST service.
We map it into CherryPy server.
"""

import cherrypy
from services.rest.RestService import RestService

# test model and formatter
from services.test.TestModel import TestModel
from services.test.TestFormatter import TestFormatter

class Service(object):
    """REST service implementation"""
    def __init__(self):
        self.rest = None # to be defined by REST class implementation
        self.verbose = 1
        self._version = "0.1.1"
    @cherrypy.expose
    def default(self, *args, **kwargs):
        """Default method for our service"""
        if  self.verbose:
            print "Called Service default"
            print args
            print kwargs
        return 
    def version(self):
        """Service version"""
        return self._version
#
# Main
#
if  __name__ == "__main__":
    SERVICE = Service()
    URL = 'http://localhost:8080/services/rest'
    SERVICE.rest = RestService()
    SERVICE.rest._model = TestModel()
    SERVICE.rest._formatter = TestFormatter()
    SERVICE.rest._url = URL
    SERVICE.rest._verbose = 1
    CONF = {'/':{'request.dispatch':cherrypy.dispatch.MethodDispatcher()}}
    cherrypy.quickstart(SERVICE, '/services', config=CONF)

