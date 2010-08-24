#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
# Author:  Valentin Kuznetsov, 2008
"""
RestServer implemention within CMS WEBTOOLS.
"""

import os
import time
import cherrypy
from cherrypy import expose
from services.rest.RestService import RestService

from utils.Utils import setsqlalchemylogger
from utils.Utils import setcherrypylogger
# test model and formatter
from services.test.TestModel import TestModel
from services.test.TestFormatter import TestFormatter

# WEBTOOLS modules
from Framework import Controller

class Service(object):
    """Service implementation, we should have some default page"""
    def __init__(self):
        self.rest = None # to be defined by REST server
        self._ver = "0.1.1" # service version
    @expose
    def default(self, *args, **kwargs):
        """Default page implementation"""
        if  kwargs.has_key('verbose') and kwargs['verbose']:
            print "REST service version %s" % self._ver
            print "REST default method invoked with:"
            print args, kwargs
        return
    def version(self):
        """Service version"""
        return self._ver

class RestServer(Controller):
    """REST server implementation within WEBTOOLS framework"""
    def __init__(self, context=None, verbose=0):
        self.name = "RestServer"
        self.baseurl = None
        if  context:
            Controller.__init__ (self, context, __file__)
            setsqlalchemylogger(super(RestServer, self).getHandler(),
                                super(RestServer, self).getLogLevel())
            setcherrypylogger(  super(RestServer, self).getHandler(),
                                super(RestServer, self).getLogLevel())
        rest_service = Service()
        rest_service.rest = RestService()
        self.rest = rest_service.rest
        # set model and formatter to be used by RestService
        self.rest._model = TestModel()
        self.rest._formatter = TestFormatter()
        self.rest._verbose = verbose
        # set server config
        self.config = self.setconfig(base=self.name)
        print "+++ %s is loaded, supported mime_types:" % self.name
        print self.rest.supporttypes

    def readyToRun(self):
        """
           this method called at run-time within WEBTOOLS framework
           it uses to define run-time parameters such as base-url, etc.
        """
        opts = self.context.CmdLineArgs().opts
        self.baseurl    = opts.baseUrl
        self.rest._url  = self.baseurl
        cherrypy.config.update ( { 
              'request.dispatch' : cherrypy.dispatch.MethodDispatcher()
                                 } )

    def setconfig(self, base=""):
        """
           Set configuration parameters for stand-along invocation
           within CherryPy server
        """
        # used thread_pool, queue_size parameters to tune
        # up server performance
        # see discussion on http://amix.dk/blog/viewEntry/119
        cherrypy.server.thread_pool = 30
        cherrypy.server.socket_queue_size = 15
        mime_types = self.rest.supporttypes
        cache_flags =  \
             'no-store, no-cache, must-revalidate,post-check=0, pre-check=0'
        httpheader = [('Expires', 
                     time.strftime( "%a, %d %b %Y %H:%M:%S GMT",
                                    time.gmtime(time.time()+315360000))
                    ),
                    ('Accept-Encoding', 'gzip'),
                    ('TE','deflate, gzip, x-gzip, identity, trailer'),
                    ('Cache-Control','max-age=315360000'),
                    ('Authorization','Basic')
                   ]
        conf = {'/' : {'request.dispatch' :cherrypy.dispatch.MethodDispatcher(),
                       'tools.staticdir.root' : os.getcwd(),
                       'tools.response_headers.on' : True,
                       'tools.etags.on' : True,
                       'tools.etags.autotags' : True,
                       'tools.response_headers.headers':
                     [('Expires', 'Mon, 26 Jul 1997 05:00:00 GMT'),
                      ('Accept-Encoding', 'gzip'),
                      ('TE', 'deflate, gzip, x-gzip, identity, trailer'),
                      ('Cache-Control', cache_flags )
                     ]
                              },
                '/images': {'tools.gzip.on' : True, 
                            'tools.gzip.mime_types' : mime_types,
                            'tools.staticdir.on' : True,
                            'tools.staticdir.root' : os.getcwd(),
                            'tools.staticdir.dir':'images',
                            'tools.response_headers.on' : True,
                            'tools.response_headers.headers' : httpheader
                           },
                '/css'   : {'tools.gzip.on' : True, 
                            'tools.gzip.mime_types' : mime_types,
                            'tools.staticdir.on' : True,
                            'tools.staticdir.root' : os.getcwd(),
                            'tools.staticdir.dir' : 'css',
                            'tools.response_headers.on' : True,
                            'tools.response_headers.headers' : httpheader
                           },
                '/js'    : {'tools.gzip.on' : True, 
                            'tools.gzip.mime_types' : mime_types,
                            'tools.staticdir.on' : True,
                            'tools.staticdir.dir' : 'js',
                            'tools.staticdir.content_types' : 
                             {'js':'text/javascript'},
                            'tools.response_headers.on' : True,
                            'tools.response_headers.headers' : httpheader
                           },
               }
        if  base:
            newconf = {}
            for key in conf.keys():
                newconf[key] = conf[key]
                if  key == "/":
                    newkey = "/%s" % base.replace("/","")
                else:
                    newkey = "/%s/%s" % (base.replace("/",""), 
                                         key.replace("/",""))
                newconf[newkey] = conf[key]
            return newconf
        return conf

