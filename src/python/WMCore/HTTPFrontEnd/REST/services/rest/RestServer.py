#!/usr/bin/env python
#-*- coding: ISO-8859-1 -*-
"""
RestServer implemention within CMS WEBTOOLS.
"""





import os
import time
import cherrypy
from cherrypy import expose
from WMCore.HTTPFrontEnd.REST.services.rest.RestService import RestService

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

class RestServer(object):
    """REST server implementation within WEBTOOLS framework"""
    def __init__(self, model, formatter, url=None, verbose=0):
        self.name = "RestServer"
        rest_service = Service()
        rest_service.rest = RestService()
        self.rest = rest_service.rest
        # check that provided model implement
        # {get,create,update,delete}data methods
        for name in ['get', 'create', 'delete', 'update']:
            clsmethod = '%sdata' % name
            try:
                getattr(model, clsmethod)
            except AttributeError:
                msg  = "\nERROR: Model class\n'%s'\n" % model
                msg += "does not implement method '%s'\n" % clsmethod
                print msg
                raise
        # check that provided formatter class implement
        # to_{xml, txt, html, json} methods
        for name in ['json', 'html', 'txt', 'xml']:
            clsmethod = 'to_%s' % name
            try:
                getattr(formatter, clsmethod)
            except AttributeError:
                msg  = "\nERROR: Formatter class\n'%s'\n" % formatter
                msg += "does not implement method '%s'\n" % clsmethod
                print msg
                raise
        # set model and formatter to be used by RestService
        self.rest._model = model
        self.rest._formatter = formatter
        self.rest._verbose = verbose
        self.rest._url = url
        # set server config
        self.config = self.setconfig(base=self.name)
        if  verbose:
            print "+++ %s is loaded, supported mime_types:" % self.name
            print self.rest.supporttypes

    def setmodel(self, model):
        """Set Model for our REST server"""
        self.rest._model = model

    def setformatter(self, formatter):
        """Set Formatter for our REST server"""
        self.rest._formatter = formatter

    def seturl(self, url):
        """Set URL for our REST server"""
        self.rest._url = url

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

