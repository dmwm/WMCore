"""

ReqMgrWorkloadCreator_t.py

Testing creating workload (WMSpec) instances from ReqMgr environment.

"""


import os
import unittest

import WMCore.RequestManager.RequestMaker.Processing.RecoRequest
import WMCore.RequestManager.RequestMaker.Processing.ReRecoRequest
import WMCore.RequestManager.RequestMaker.Processing.DataProcessingRequest
import WMCore.RequestManager.RequestMaker.Production.MonteCarloRequest

import WMCore.WMSpec.StdSpecs.ReReco as ReRecoSpec
import WMCore.WMSpec.StdSpecs.RelValMC as RelValMCSpec
import WMCore.WMSpec.StdSpecs.StoreResults as StoreResultsSpec
import WMCore.WMSpec.StdSpecs.MonteCarlo as MonteCarloSpec
import WMCore.WMSpec.StdSpecs.DataProcessing as DataProcessingSpec

from WMCore.RequestManager.RequestMaker.Registry import retrieveRequestMaker
from WMQuality.TestInitCouchApp import TestInitCouchApp as TestInit
from WMCore.Services.UUID import makeUUID
from WMCore.Cache.WMConfigCache import ConfigCache



class ReqMgrWorkloadCreatorTest(unittest.TestCase):
    def setUp(self):
        """
        Set up the base schema.
        
        """
        self.baseSchema = {'Requestor': 'me', 'Group': 'us'}
        self.couchDB = 'makeworkload_t'        
        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        self.testInit.setupCouch(self.couchDB, "GroupUser", "ConfigCache")
        self.configCache = self.setupConfigCache()


    def tearDown(self):
        """
        Tear down the DB.
        
        """
        self.testInit.clearDatabase()
        self.testInit.tearDownCouch()

    def setupConfigCache(self):
        """
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


    def _performTest(self, name, schema):
        """
        Actually do the work of constructing code.
        
        """
        schema.update(self.baseSchema)
        schema['RequestName'] = name
        schema['ProdConfigCacheID'] = self.configCache.getCouchID()
        schema['CouchDBName'] = self.couchDB
        
        if 'GenConfigCacheID' in schema.keys():
            schema['GenConfigCacheID'] = self.configCache.getCouchID()
            schema['StepOneConfigCacheID'] = self.configCache.getCouchID()
            schema['StepTwoConfigCacheID'] = self.configCache.getCouchID()
        maker = retrieveRequestMaker(name)
        request = maker(schema)


    def testReReco(self):
        self._performTest('ReReco', ReRecoSpec.getTestArguments())


    def testMonteCarlo(self):
        self._performTest('MonteCarlo', MonteCarloSpec.getTestArguments())


    def testRelValMC(self):
        schema = RelValMCSpec.getTestArguments()
        # fix, otherwise fist 1st chaining in RelValMC fails
        # (i.e.: stepOneTask = genMergeTask.addTask("StepOne"))
        schema["GenOutputModuleName"] = "ThisIsAName"
        # fix, otherwise 2nd chaining in RelValMC fails
        # (i.e.: stepTwoTask = stepOneMergeTask.addTask("StepTwo"))
        schema["StepOneOutputModuleName"] = "ThisIsAName"        
        self._performTest('RelValMC', schema)


    def testStoreResults(self):
        self._performTest('StoreResults', StoreResultsSpec.getTestArguments())

    
    def testDataProcessing(self):
        self._performTest('DataProcessing', DataProcessingSpec.getTestArguments())



if __name__=='__main__':
    unittest.main()