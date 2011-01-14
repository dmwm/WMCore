import unittest
import cherrypy
import logging

#decorator import for RESTServer setup
from WMQuality.TestInitCouchApp import TestInitCouchApp
from WMQuality.WebTools.RESTBaseUnitTest import RESTBaseUnitTest
from WMQuality.WebTools.RESTServerSetup import DefaultConfig
from WMCore.WebTools.Root import Root

class RESTAndCouchUnitTest(RESTBaseUnitTest):
    
    def setUp(self):
        # default set
        if self.schemaModules:
            self.testInit = TestInitCouchApp(__file__)
        RESTBaseUnitTest.__init__(self)