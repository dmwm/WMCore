import WMCore_t.RequestManager_t.FakeRequests as FakeRequests
import WMCore.RequestManager.RequestMaker.Processing.RecoRequest
import WMCore.RequestManager.RequestMaker.Processing.ReRecoRequest
import WMCore.RequestManager.RequestMaker.Processing.FileBasedRequest
import WMCore.RequestManager.RequestMaker.Production.MonteCarloRequest
import WMCore.WMSpec.StdSpecs.ReReco as ReRecoSpec
import WMCore.WMSpec.StdSpecs.RelValMC as RelValMCSpec
import WMCore.WMSpec.StdSpecs.StoreResults as StoreResultsSpec
import WMCore.WMSpec.StdSpecs.MonteCarlo as MonteCarloSpec

from WMCore.RequestManager.RequestMaker.Registry import retrieveRequestMaker

import unittest

class makeWorkload_t(unittest.TestCase):
    def setUp(self):
        self.baseSchema = {'Requestor': 'me', 'Group': 'us'}

    def do(self, name, schema):
        schema.update(self.baseSchema)
        schema['RequestName'] = name
        maker = retrieveRequestMaker(name)
        request = maker(schema)

    def testReReco(self):
        self.do('ReReco', ReRecoSpec.getTestArguments())

    def testMonteCarlo(self):
        self.do('MonteCarlo', MonteCarloSpec.getTestArguments())

    def testRelValMC(self):
        self.do('RelValMC', RelValMCSpec.getTestArguments())

    def testStoreResults(self):
        self.do('StoreResults', StoreResultsSpec.getTestArguments())

    def tearDown(self):
        pass

if __name__=='__main__':
    unittest.main()
