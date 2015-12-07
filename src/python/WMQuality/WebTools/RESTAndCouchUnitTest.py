#decorator import for RESTServer setup
from WMQuality.TestInitCouchApp import TestInitCouchApp
from WMQuality.WebTools.RESTBaseUnitTest import RESTBaseUnitTest

class RESTAndCouchUnitTest(RESTBaseUnitTest):

    def setUp(self):
        # default set
        if self.schemaModules:
            self.testInit = TestInitCouchApp(__file__)
        RESTBaseUnitTest.__init__(self)
