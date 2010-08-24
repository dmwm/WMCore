#!/usr/bin/env python

import os
import sys
import cherrypy
from cherrypy import expose
import types
from services.rest import *
from services.rest.RestService import *

# test model and formatter
from services.test.TestModel import *
from services.test.TestFormatter import *

# specific modules
from utils.webtools_modules import *
from utils.Utils import *

class Service(object):
    @expose
    def default(self, *args, **kwargs):
        return

class RestServer(Controller):
    def __init__(self,context=None,verbose=0):
        self.name="RestServer"
        if context:
            Controller.__init__ (self, context, __file__)
#            setSQLAlchemyLogger(super(RestServer,self).getHandler(),
#                                super(RestServer,self).getLogLevel())
#            setCherryPyLogger(  super(RestServer,self).getHandler(),
#                                super(RestServer,self).getLogLevel())
        rest_service = Service()
        rest_service.rest = RestService()
        self.rest = rest_service.rest
        # set model and formatter to be used by RestService
        self.rest._model = TestModel()
        self.rest._formatter = TestFormatter()
        self.rest._verbose = verbose
        # set server config
        self.config=self.setConfig(base=self.name)
        print "+++ %s is loaded, supported mime_types:"%self.name
        print self.rest.supportTypes

    def readyToRun(self):
        opts=self.context.CmdLineArgs().opts
        self.baseUrl   = opts.baseUrl
        self.rest._url = opts.baseUrl
        self.rest._mUrl= self.baseUrl+"base/Common/masthead"
        self.rest._fUrl= self.baseUrl+"base/Common/footer"
        self.rest._host= self.baseUrl
        cherrypy.config.update ({'request.dispatch':cherrypy.dispatch.MethodDispatcher()})

    def setConfig(self,base=""):
        # used thread_pool, queue_size parameters to tune up server performance
        # see discussion on http://amix.dk/blog/viewEntry/119
        cherrypy.server.thread_pool = 30
        cherrypy.server.socket_queue_size = 15
        mime_types=self.rest.supportTypes
        httpHeader=[('Expires',time.strftime("%a, %d %b %Y %H:%M:%S GMT",time.gmtime(time.time()+315360000))),
                               ('Accept-Encoding','gzip'),
                               ('TE','deflate, gzip, x-gzip, identity, trailer'),
                               ('Cache-Control','max-age=315360000'),
                               ('Authorization','Basic')
                   ]
        conf = {'/'         : {'request.dispatch':cherrypy.dispatch.MethodDispatcher(),
                               'tools.staticdir.root': os.getcwd(),
                               'tools.response_headers.on':True,
                               'tools.etags.on':True,
                               'tools.etags.autotags':True,
                               'tools.response_headers.headers':
                              [('Expires','Mon, 26 Jul 1997 05:00:00 GMT'),
                               ('Accept-Encoding','gzip'),
                               ('TE','deflate, gzip, x-gzip, identity, trailer'),
                               ('Cache-Control','no-store, no-cache, must-revalidate,post-check=0, pre-check=0')]
                              },
                '/images'   : {'tools.gzip.on': True, 
                               'tools.gzip.mime_types':mime_types,
                               'tools.staticdir.on':True,
                               'tools.staticdir.root': os.getcwd(),
                               'tools.staticdir.dir':'images',
                               'tools.response_headers.on':True,
                               'tools.response_headers.headers':httpHeader
                              },
                '/css'      : {'tools.gzip.on': True, 
                               'tools.gzip.mime_types':mime_types,
                               'tools.staticdir.on':True,
                               'tools.staticdir.root': os.getcwd(),
                               'tools.staticdir.dir':'css',
                               'tools.response_headers.on':True,
                               'tools.response_headers.headers':httpHeader
                              },
                '/js'       : {'tools.gzip.on': True, 
                               'tools.gzip.mime_types':mime_types,
                               'tools.staticdir.on':True,
                               'tools.staticdir.dir':'js',
                               'tools.staticdir.content_types':{'js':'text/javascript'},
                               'tools.response_headers.on':True,
                               'tools.response_headers.headers':httpHeader
                              },
               }
        if  base:
            newConf={}
            for key in conf.keys():
                newConf[key]=conf[key]
                if key=="/":
                   newKey="/%s"%base.replace("/","")
                else:
                   newKey="/%s/%s"%(base.replace("/",""),key.replace("/",""))
                newConf[newKey]=conf[key]
            return newConf
        return conf

#
# Main
#
if __name__=="__main__":
    DDRest = RestServer(context,verbose=0)
    conf = {'/':{'request.dispatch':cherrypy.dispatch.MethodDispatcher()}}
    cherrypy.quickstart(rest_service,'/service',config=conf)
