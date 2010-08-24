#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
"""
This is an example of how to implement simple REST service.
We define Service class who hold the REST service.
We map it into CherryPy server.
"""

__author__ = "Valentin Kuznetsov <vkuznet at gmail dot com>"
__revision__ = "$Id:"
__version__ = "$Revision:"

import cherrypy
from services.rest.RestService import RestService

# test model and formatter
from WMCore.HTTPFrontEnd.REST.services.test.TestModel import TestModel
from WMCore.HTTPFrontEnd.REST.services.test.TestFormatter import TestFormatter

class Service(object):
    """REST service implementation"""
    def __init__(self):
        self.rest = None # to be defined by REST class implementation
        self.verbose = 0
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

def restservice():
    """REST service implementation in CherryPy web server"""
    service = Service()
    url = 'http://localhost:8080/services/rest'
    service.rest = RestService()
    service.rest._model = TestModel()
    service.rest._formatter = TestFormatter()
    service.rest._url = url
    service.rest._verbose = 0
    conf = {'/':{'request.dispatch':cherrypy.dispatch.MethodDispatcher()}}
    cherrypy.quickstart(service, '/services', config=conf)
#
# Main
#
if  __name__ == "__main__":
    restservice()
