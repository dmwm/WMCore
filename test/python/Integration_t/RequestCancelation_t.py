#from Integration_t.ReRecoLifeCycle_t import ReRecoLifeCycle_t
from Integration_t.RequestLifeCycleBase_t import RequestLifeCycleBase_t

import time
import unittest
import nose
from nose.plugins.attrib import attr


class RequestCancellation_t(RequestLifeCycleBase_t, unittest.TestCase):

    requestParams = {'CMSSWVersion' : 'CMSSW_4_4_2_patch2',
                        "Scenario": "pp", "inputMode" : "Scenario", "ProcScenario" : "pp",
                        "RequestType": "ReReco",
                        "Requestor": 'integration',
                        "InputDataset": "/BTag/Run2011B-v1/RAW", "RunWhitelist" : [177316, 177317],
                        "GlobalTag": 'FT_R_44_V11::All', 'Group' : 'DMWM', "RequestPriority" : 10,
                    }

    # instead of checking its running cancel it.
    @attr("lifecycle")
    def test60CancelRequest(self):
        """Cancel workflow"""
        self.__class__.reqmgr.reportRequestStatus(self.__class__.request_name, 'aborted')
        self.__class__.request = self.__class__.reqmgr.getRequest(self.__class__.request_name)
        self.assertEqual(self.__class__.request['RequestStatus'], 'aborted')

    @attr("lifecycle")
    def test70WorkQueueFinished(self):
        """Request canceled in workqueue"""
        if not self.__class__.request_name:
            raise nose.SkipTest
        start = time.time()
        while True:
            request = [x for x in self.__class__.workqueue.getElementsCountAndJobsByWorkflow() if \
                    x == self.__class__.request_name]
            # request deleted from wq shortly after finishing, so may not appear here
            if not request or request == [x for x in request if x['status'] in ('Canceled')]:
                break
            if start + (60 * 20) < time.time():
                raise RuntimeError('timeout waiting for request to finish')
            time.sleep(15)

    @attr("lifecycle")
    def test80RequestFinished(self):
        """Request canceled"""
        if not self.__class__.request_name:
            raise nose.SkipTest
        start = time.time()
        while True:
            self.__class__.request = self.__class__.reqmgr.getRequest(self.__class__.request_name)
            if self.__class__.request['RequestStatus'] == 'aborted':
                break
            if start + (60 * 20) < time.time():
                raise RuntimeError('timeout waiting for request to finish')
            time.sleep(15)

# Remove tests that don't apply
delattr(RequestLifeCycleBase_t, 'test60RequestRunning')
delattr(RequestLifeCycleBase_t, 'test90RequestCloseOut')

if __name__ == "__main__":
    unittest.main()
