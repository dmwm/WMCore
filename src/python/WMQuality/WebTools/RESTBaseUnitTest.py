from __future__ import print_function

import logging
import threading
import traceback
import unittest

import cherrypy
import cherrypy.process.wspbus as cherrybus

# decorator import for RESTServer setup
from WMCore.WebTools.Root import Root


class RESTBaseUnitTest(unittest.TestCase):
    def setUp(self, initRoot=True):
        # default set
        self.schemaModules = []

        self.initialize()
        if self.schemaModules:
            import warnings
            warnings.warn("use RESTAndCouchUnitTest instead", DeprecationWarning)
            from WMQuality.TestInitCouchApp import TestInitCouchApp
            self.testInit = TestInitCouchApp(__file__)
            self.testInit.setLogging()  # logLevel = logging.SQLDEBUG
            self.testInit.setDatabaseConnection(destroyAllDatabase=True)
            self.testInit.setSchema(customModules=self.schemaModules,
                                    useDefault=False)
            # Now pull the dbURL from the factory
            # I prefer this method because the factory has better error handling
            # Also because then you know everything is the same
            myThread = threading.currentThread()
            self.config.setDBUrl(myThread.dbFactory.dburl)

        logging.info("This is our config: %s" % self.config)

        self.initRoot = initRoot
        if initRoot:
            self.rt = Root(self.config, testName=self._testMethodName)
            try:
                self.rt.start(blocking=False)
            except RuntimeError as e:
                # there appears to be worker threads from a previous test
                # hanging out. Try to slay them so that we can keep going
                print("Failed to load cherrypy with exception: %s\n" % e)
                print("The threads are: \n%s\n" % threading.enumerate())
                print("The previous test was %s\n" % self.rt.getLastTest())
                print(traceback.format_exc())
                self.rt.stop()
                raise e

        return

    def tearDown(self):
        if self.initRoot:
            self.rt.stop()
            self.rt.setLastTest()
            # there was a ton of racy failures in REST tools because of
            # how much global state cherrypy has. this resets it

            # Also, it sucks I had to copy/paste this from
            # https://bitbucket.org/cherrypy/cherrypy/src/9720342ad159/cherrypy/__init__.py
            # but reload() doesn't have the right semantics

            cherrybus.bus = cherrybus.Bus()
            cherrypy.engine = cherrybus.bus
            # This class has apparently been deprecated in the newer versions
            # cherrypy.engine.timeout_monitor = cherrypy._TimeoutMonitor(cherrypy.engine)
            # cherrypy.engine.timeout_monitor.subscribe()

            cherrypy.engine.autoreload = cherrypy.process.plugins.Autoreloader(cherrypy.engine)
            cherrypy.engine.autoreload.subscribe()

            cherrypy.engine.thread_manager = cherrypy.process.plugins.ThreadManager(cherrypy.engine)
            cherrypy.engine.thread_manager.subscribe()

            cherrypy.engine.signal_handler = cherrypy.process.plugins.SignalHandler(cherrypy.engine)
            cherrypy.engine.subscribe('log', cherrypy._buslog)

            from cherrypy import _cpserver
            cherrypy.server = _cpserver.Server()
            cherrypy.server.subscribe()
            cherrypy.checker = cherrypy._cpchecker.Checker()
            cherrypy.engine.subscribe('start', cherrypy.checker)

        if self.schemaModules:
            self.testInit.clearDatabase()
        self.config = None
        return

    def initialize(self):
        """
        i.e.

        self.config = DefaultConfig('WMCore.WebTools.RESTModel')
        self.config.setDBUrl('sqlite://')
        self.schemaModules = ['WMCore.ThreadPool', 'WMCore.WMBS']
        """

        message = "initialize method has to be implemented, self.restModel, self.schemaModules needs to be set"
        raise NotImplementedError(message)
