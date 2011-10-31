#!/usr/bin/env pythong
import os
import unittest

from nose.plugins.attrib import attr

import WMCore.RequestManager.RequestMaker.Processing.RecoRequest
import WMCore.RequestManager.RequestMaker.Processing.ReRecoRequest
import WMCore.RequestManager.RequestMaker.Processing.DataProcessingRequest
import WMCore.RequestManager.RequestMaker.Production.MonteCarloRequest

import WMCore.WMSpec.StdSpecs.ReReco          as ReRecoSpec
import WMCore.WMSpec.StdSpecs.RelValMC        as RelValMCSpec
import WMCore.WMSpec.StdSpecs.StoreResults    as StoreResultsSpec
import WMCore.WMSpec.StdSpecs.MonteCarlo      as MonteCarloSpec
import WMCore.WMSpec.StdSpecs.DataProcessing  as DataProcessingSpec

from WMCore.RequestManager.RequestMaker.Registry import retrieveRequestMaker
from WMQuality.TestInitCouchApp                  import TestInitCouchApp as TestInit
from WMCore.Services.UUID                        import makeUUID
from WMCore.Cache.WMConfigCache                  import ConfigCache


class makeWorkload_t(unittest.TestCase):
    def setUp(self):
        """
        _setUp_

        Set up the base schema
        """
        self.baseSchema = {'Requestor': 'me', 'Group': 'us'}
        self.couchDB    = 'makeworkload_t'        

        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setupCouch(self.couchDB, "GroupUser", "ConfigCache")

        self.configCache = self.setupConfigCache()
        return

    def tearDown(self):
        """
        _tearDown_

        Tear down the DB
        """

        self.testInit.clearDatabase()
        self.testInit.tearDownCouch()
        return

    def setupConfigCache(self):
        """
        _setupConfigCache_
        
        Setup a config cache object that we can load out later.
        """

        PSetTweak = {'process': {'outputModules_': ['ThisIsAName'],
                                 'ThisIsAName': {'dataset': {'dataTier': 'RECO',
                                                             'filterName': 'Filter'}}}}

        configCache = ConfigCache(os.environ["COUCHURL"], couchDBName = self.couchDB)
        configCache.createUserGroup(groupname = "testGroup", username = 'testOps')
        configCache.setPSetTweaks(PSetTweak = PSetTweak)
        configCache.save()

        return configCache

    def do(self, name, schema):
        """
        _do_

        Actually do the work of constructing code
        """
        schema.update(self.baseSchema)
        schema['RequestName']       = name
        schema['ProdConfigCacheID'] = self.configCache.getCouchID()
        schema['CouchDBName']       = self.couchDB

        if 'GenConfigCacheID' in schema.keys():
            schema['GenConfigCacheID']     = self.configCache.getCouchID()
            schema['StepOneConfigCacheID'] = self.configCache.getCouchID()
            schema['StepTwoConfigCacheID'] = self.configCache.getCouchID()
        maker = retrieveRequestMaker(name)
        request = maker(schema)
        return

    @attr("integration")
    def testReReco(self):
        self.do('ReReco', ReRecoSpec.getTestArguments())

    @attr("integration")
    def testMonteCarlo(self):
        self.do('MonteCarlo', MonteCarloSpec.getTestArguments())

    @attr("integration")
    def testRelValMC(self):
        self.do('RelValMC', RelValMCSpec.getTestArguments())

    @attr("integration")
    def testStoreResults(self):
        self.do('StoreResults', StoreResultsSpec.getTestArguments())

    @attr("integration")
    def testDataProcessing(self):
        self.do('DataProcessing', DataProcessingSpec.getTestArguments())

if __name__=='__main__':
    unittest.main()
