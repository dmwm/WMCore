#!/usr/bin/env python

import cherrypy
from services.rest import *
from services.rest.RestService import *

# test model and formatter
from services.test.TestModel import *
from services.test.TestFormatter import *

class Service(object):
    @cherrypy.expose
    def default(self, *args, **kwargs):
        print "Called Service default"
        print args
        print kwargs
        return 
#
# Main
#
if __name__=="__main__":
   rest_service = Service()
   url='http://localhost:8080/services/rest'
   rest_service.rest = RestService()
   rest_service.rest._model = TestModel()
   rest_service.rest._formatter = TestFormatter()
   rest_service.rest._url = url
   conf = {'/':{'request.dispatch':cherrypy.dispatch.MethodDispatcher()}}
   cherrypy.quickstart(rest_service,'/services',config=conf)

