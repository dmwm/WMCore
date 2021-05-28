from builtins import object
from future.utils import viewitems, viewvalues

import os
import cherrypy
import logging
from functools import wraps

from WMCore.REST.Main import RESTMain
from WMCore.REST.Test import fake_authz_headers, fake_authz_key_file
from WMCore.Services.Requests import JSONRequests

class RESTMainTestServer(object):

    def __init__(self, cfg, statedir, testName):
        self.server = RESTMain(cfg, statedir)
        self.testName = testName
        self.config = cfg
        self.port = cfg.main.port
        self.host = '127.0.0.1'
        self.serverUrl = "http://%s:%s/%s/" % (self.host, self.port, cfg.main.application)

        ## test authentication using fake filepermission
        self.test_authz_key = fake_authz_key_file(False)
        self.config.main.tools.cms_auth.key_file = self.test_authz_key.name
        #self.header = fake_authz_headers(test_authz_key.data, roles = {"Admin": {'group': ['ReqMgr']}})

        self.jsonSender = JSONRequests(self.serverUrl)

    def getLastTest(self):
        global lastTest
        return lastTest

    def setLastTest(self):
        global lastTest
        lastTest = self.testName


    def start(self, blocking = True):
        self.server.validate_config()
        self.server.setup_server()
        self.server.install_application()
        cherrypy.config.update({'server.socket_port': self.port})
        cherrypy.config.update({'server.socket_host': self.host})
        cherrypy.config.update({'request.show_tracebacks': True})
        cherrypy.config.update({'environment': 'test_suite'})
        cherrypy.config.update({'server.thread_pool': 10})
        cherrypy.config.update({'server.accepted_queue_size': -1})
        cherrypy.config.update({'server.accepted_queue_timeout': 10})
        for app in viewvalues(cherrypy.tree.apps):
            if '/' in app.config:
                app.config["/"]["request.show_tracebacks"] = True

        cherrypy.server.httpserver = None
        cherrypy.engine.start()
        if blocking:
            cherrypy.engine.block()


    def stop(self):
        """
        Stop the server
        """
        cherrypy.engine.exit()
        cherrypy.engine.stop()

        # Ensure the next server that's started gets fresh objects
        for name, server in viewitems(getattr(cherrypy, 'servers', {})):
            server.unsubscribe()
            del cherrypy.servers[name]

        self.test_authz_key.close()
        os.remove(self.test_authz_key.name)
