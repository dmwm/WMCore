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
from services.rest.RestServer import RestServer

# test model and formatter
from WMCore.HTTPFrontEnd.REST.services.test.TestModel import TestModel
from WMCore.HTTPFrontEnd.REST.services.test.TestFormatter import TestFormatter

def restservice():
    """REST service implementation in CherryPy web server"""
    url = 'http://localhost:8080/services/rest'
    model = TestModel()
    formatter = TestFormatter()
    verbose = 0
    rest = RestServer(model, formatter, url, verbose)
    conf = {'/':{'request.dispatch':cherrypy.dispatch.MethodDispatcher()}}
    cherrypy.quickstart(rest, '/services', config=conf)
#
# Main
#
if  __name__ == "__main__":
    restservice()
