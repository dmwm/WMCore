import WMCore_t.RequestManager_t.FakeRequests as FakeRequests
import WMCore.RequestManager.RequestMaker.Processing.RecoRequest
import WMCore.RequestManager.RequestMaker.Processing.ReRecoRequest
import WMCore.RequestManager.RequestMaker.Processing.FileBasedRequest
import WMCore.RequestManager.RequestMaker.Production.MonteCarloRequest
import WMCore.WMSpec.StdSpecs.ReReco as ReRecoSpec
import WMCore.WMSpec.StdSpecs.MonteCarlo as MonteCarloSpec

from WMCore.RequestManager.RequestMaker.Registry import retrieveRequestMaker

import unittest

class makeWorkload_t(unittest.TestCase):
    def setUp(self):
        pass

    def testReReco(self):
        schema = ReRecoSpec.getTestArguments()
        schema['RequestName'] = 'ReRecoTest'
        maker = retrieveRequestMaker('ReReco')
        request = maker.makeWorkload(schema)

    def testMonteCarlo(self):
        schema = MonteCarloSpec.getTestArguments()
        schema['RequestName'] = 'MonteCarloTest'
        maker = retrieveRequestMaker('MonteCarlo')
        request = maker.makeWorkload(schema)


    def tearDown(self):
        pass

if __name__=='__main__':
    unittest.main()
