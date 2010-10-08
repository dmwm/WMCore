import WMCore_t.RequestManager_t.FakeRequests as FakeRequests
import WMCore.RequestManager.RequestMaker.Processing.RecoRequest
import WMCore.RequestManager.RequestMaker.Processing.ReRecoRequest
import WMCore.RequestManager.RequestMaker.Processing.FileBasedRequest
import WMCore.RequestManager.RequestMaker.Production.MonteCarloRequest
from WMCore.RequestManager.RequestMaker.Registry import retrieveRequestMaker

import unittest

class makeWorkload_t(unittest.TestCase):
    def setUp(self):
        pass

    def testMakeWorkload(self):
        for requestType in ['ReReco', 'MonteCarlo', 'FileProcessing']:
            schema = FakeRequests.fakeRequest(requestType)
            maker = retrieveRequestMaker(requestType)
            request = maker.makeWorkload(schema)


    def tearDown(self):
        pass

if __name__=='__main__':
    unittest.main()
