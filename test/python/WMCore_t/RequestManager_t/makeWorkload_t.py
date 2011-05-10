import WMCore_t.RequestManager_t.FakeRequests as FakeRequests
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

from nose.plugins.attrib import attr

import unittest

class makeWorkload_t(unittest.TestCase):
    def setUp(self):
        self.baseSchema = {'Requestor': 'me', 'Group': 'us'}

    def do(self, name, schema):
        schema.update(self.baseSchema)
        schema['RequestName'] = name
        schema['CouchDBName'] = 'reqmgr_config_cache'
        schema['ProdConfigCacheID'] = '0582a460e28d54ce6b8f1a14845be0da'
        maker = retrieveRequestMaker(name)
        request = maker(schema)

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

    def tearDown(self):
        pass

if __name__=='__main__':
    unittest.main()
