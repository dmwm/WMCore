from __future__ import print_function
import unittest
import cherrypy
import logging
import threading
import os

#decorator import for RESTServer setup
from WMQuality.REST.ServerSetup import RESTMainTestServer
import WMQuality.CherrypyTestInit as CherrypyTestInit


class RESTBaseUnitTestWithDBBackend(unittest.TestCase):

    def setUp(self, initRoot = True):
        """
            unittest inherits this class
            should have set
            self.setConfig(config)
            config is WMCore.REST application config

            i.e.
            class ClildClass(RESTBaseUnitTestWithDBBackend):

                def setUp(self):
                    self.setConfig(WMCore.ReqMgr.Config)
                    # following setters are optional
                    self.setCouchDBs([("reqmgr_workload_config", "ReqMgr")])
                    self.setSchemaModules(["WMCore.WMBS"])


        """
        if self.schemaModules or self.couchDBs:
            from WMQuality.TestInitCouchApp import TestInitCouchApp
            self.testInit = TestInitCouchApp(__file__)
            self.testInit.setLogging() # logLevel = logging.SQLDEBUG

            if self.schemaModules:
                self.testInit.setDatabaseConnection()
                self.testInit.setSchema(customModules = self.schemaModules,
                                        useDefault = False)
                # Now pull the dbURL from the factory
                # I prefer this method because the factory has better error handling
                # Also because then you know everything is the same
                myThread = threading.currentThread()
                self.config.setDBUrl(myThread.dbFactory.dburl)

            if self.couchDBs:
                for (dbName, couchApp) in self.couchDBs:
                    if couchApp:
                        self.testInit.setupCouch(dbName, couchApp)
                    else:
                        self.testInit.setupCouch(dbName)


        logging.info("RESTBaseUnitTestWithDBBackend configuration: %s" % self.config)

        self.initRoot = initRoot
        if initRoot:
            self.server = RESTMainTestServer(self.config, os.getcwd(), self._testMethodName)
            CherrypyTestInit.start(self.server)
            self.jsonSender = self.server.jsonSender
            # find the way to check the api with the permission
            self.test_authz_key = self.server.test_authz_key
            print("init root")

    def tearDown(self):
        logging.info("RESTBaseUnitTestWithDBBackend executing tearDown")
        if self.initRoot:
            CherrypyTestInit.stop(self.server)
            self.test_authz_key = None

        if self.schemaModules:
            self.testInit.clearDatabase()

        if self.couchDBs:
            self.testInit.tearDownCouch()

        self.config = None
        self.jsonSender = None
        return

    def setSchemaModules(self, schemaModules):
        """
        This need to be set if backend db connection is needed
        ie.
        schemaModules = ["WMCore.WMBS","WMComponent.DBS3Buffer","WMCore.BossAir"]
        """
        self.schemaModules = schemaModules or []

    def setCouchDBs(self, couchDBs):
        """
        This need to be set if counchdb connection is needed
        couchDBs = [("reqmgr_workload_config", "ReqMgr"),  ]
        """
        self.couchDBs = couchDBs or []

    def setConfig(self, config):
        self.config = config
