"""
Unit tests for Unified/Transferor.py module

Author: Valentin Kuznetsov <vkuznet [AT] gmail [DOT] com>
"""
from __future__ import division, print_function

# system modules
import time
import unittest

# WMCore modules
# from WMCore.Services.PhEDEx.PhEDEx import PhEDEx
from WMCore.MicroService.Unified.Transferor import MSManager, Services
from WMQuality.Emulators.PhEDExClient.MockPhEDExApi import MockPhEDExApi
from WMQuality.Emulators.EmulatedUnitTestCase import EmulatedUnitTestCase

class TransferorTest(EmulatedUnitTestCase):
    "Unit test for Transferor module"
    def setUp(self):
        "init test class"
        #super(TransferorTest, self).setUp()
        self.group = 'DataOps'
        self.interval = 2
        self.phedex = MockPhEDExApi()
        reqmgrUrl = 'http://localhost'
        svc = Services(reqmgrUrl)
        self.rmgr = MSManager(svc, group=self.group, interval=self.interval)

        # get some subscriptions from PhEDEx to play with
        data = self.phedex.subscriptions(group=self.group)
        print("### data", data)
        for datasetInfo in data['phedex']['dataset']:
            dataset = datasetInfo.get('name')
            print("### dataset info from phedex, dataset %s #files %s" \
                    % (dataset, datasetInfo.get('files', 0)))
            # now use the same logic in as in Transferor, i.e. look-up dataset/group subscription
            data = self.phedex.subscriptions(dataset=dataset, group=self.group)
            if not data['phedex']['dataset']:
                print("### skip this dataset since no subscription data is available")
                continue
            nodes = [i['node'] for r in data['phedex']['dataset'] for i in r['subscription']]
            print("### nodes", nodes)
            # create fake requests with dataset/nodes info
            rdict1 = dict(datasets=[dataset], sites=nodes, name='req1')
            rdict2 = dict(datasets=[dataset], sites=nodes, name='req2')
            self.requests = [rdict1, rdict2]
            print("+++ stored req1", self.rmgr.info({'req1': rdict1}))
            print("+++ stored req2", self.rmgr.info({'req2': rdict2}))
            break

    def tearDown(self):
        "tear down all resources and exit unit test"
        self.rmgr.stop() # stop internal thread

    def testMSManager(self):
        "Test function for MSManager class"
        # add requests to MSManager
        print("+++ store", self.requests)
        for req in self.requests:
            print("+++ store", self.rmgr.info(req))
            # check their status
            # after fetch request info here it will be gone from store
#             info = self.rmgr.info(req)
#             completed = info.pop('completed')
#             self.assertEqual(100, int(completed))
#             self.assertEqual(self.requests[request], info)
#             self.rmgr.checkStatus(request)
            # at this point request should be gone from store
#             self.assertEqual(False, self.rmgr.store.exists(request))
            # but we can check request status as many times as we want
#             self.rmgr.checkStatus(request)

    def testMSManagerAutomation(self):
        "Test function for MSManager class which check status of request automatically"
        # add requests to MSManager
        print("+++ store", self.requests)
        for req in self.requests:
            print("+++ store", self.rmgr.info(req))
        # we'll sleep and allow MSManager thread to check status of requests
        # and wipe out them from internal store
        time.sleep(self.interval+1)
        # check their status
        for req in self.requests:
            # but we can check request status as many times as we want
            self.rmgr.checkStatus(req)


if __name__ == '__main__':
    unittest.main()
