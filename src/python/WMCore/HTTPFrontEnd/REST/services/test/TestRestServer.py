#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
"""
This is an example of how to implement simple REST service.
We define Service class who hold the REST service.
We map it into CherryPy server.
"""





import cherrypy
from WMCore.HTTPFrontEnd.REST.services.rest.RestServer import RestServer

# test model and formatter
from WMCore.HTTPFrontEnd.REST.services.test.TestModel import TestModel
from WMCore.HTTPFrontEnd.REST.services.test.TestFormatter import TestFormatter

def restservice():
    """REST service implementation in CherryPy web server"""
    url = 'http://localhost:8089/services/rest'
    model = TestModel()
    formatter = TestFormatter()
    verbose = 0
    rest = RestServer(model, formatter, url, verbose)
    conf = {'/':{'request.dispatch':cherrypy.dispatch.MethodDispatcher()}}
    cherrypy.server.socket_port = 8089
    cherrypy.quickstart(rest, '/services', config=conf)
#
# Main
#
if  __name__ == "__main__":
    restservice()
