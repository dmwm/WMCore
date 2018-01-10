"""
Unit tests for Unified/Transferor.py module

Author: Valentin Kuznetsov <vkuznet [AT] gmail [DOT] com>
"""
from __future__ import division, print_function

# system modules
import time
import tempfile
import unittest

# WMCore modules
from WMCore.Services.PhEDEx.PhEDEx import PhEDEx
from WMCore.MicroService.Unified.Transferor import \
        RequestStore, RequestManager
from WMQuality.Emulators.PhEDExClient.MockPhEDExApi import MockPhEDExApi
from WMQuality.Emulators.EmulatedUnitTestCase import EmulatedUnitTestCase

class TransferorTest(EmulatedUnitTestCase):
    "Unit test for Transferor module"
    def setUp(self):
        "init test class"
        super(TransferorTest, self).setUp()
        self.group = 'DataOps'
        self.interval = 2
        self.phedex = MockPhEDExApi()
        self.rmgr = RequestManager(group=self.group, interval=self.interval, verbose=True)

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
            self.requests = {'req1': rdict1, 'req2': rdict2}
            print("+++ stored requests", self.rmgr.info())
            break

    def tearDown(self):
        "tear down all resources and exit unit test"
        self.rmgr.stop() # stop internal thread

    def testRequestManager(self):
        "Test function for RequestManager class"
        # add requests to RequestManager
        print("+++ store", self.requests)
        self.rmgr.add(self.requests)
        print("+++ store", self.rmgr.info())
        # check their status
        for request in self.requests.keys():
            # after fetch request info here it will be gone from store
            info = self.rmgr.info(request)
            print("### request", request, "info", info)
            completed = info.pop('completed')
            self.assertEqual(100, int(completed))
            self.assertEqual(self.requests[request], info)
            self.rmgr.checkStatus(request)
            # at this point request should be gone from store
            self.assertEqual(False, self.rmgr.store.exists(request))
            # but we can check request status as many times as we want
            self.rmgr.checkStatus(request)

    def testRequestManagerAutomation(self):
        "Test function for RequestManager class which check status of request automatically"
        # add requests to RequestManager
        print("+++ store", self.requests)
        self.rmgr.add(self.requests)
        print("+++ store", self.rmgr.info())
        # we'll sleep and allow RequestManager thread to check status of requests
        # and wipe out them from internal store
        time.sleep(self.interval+1)
        # check their status
        for request in self.requests.keys():
            # at this point request should be gone from store
            self.assertEqual(False, self.rmgr.store.exists(request))
            # but we can check request status as many times as we want
            self.rmgr.checkStatus(request)

    def testRequestStore(self):
        "Test function for RequestStore()"
        fobj = tempfile.NamedTemporaryFile()
        fname = '%s.db' % fobj.name
        print("### open store", fname)
        store = RequestStore(fname)
        requests = [{'bla': {'meta': 'bla'}}, {'foo': {'meta': 'foo'}}]
        for item in requests:
            for request, rdict in item.items():
                store.add(request, rdict)
                self.assertEqual(True, store.exists(request))
                print("### request: %s" % request, "store info: ", store.info(request))
        store.delete('bla')
        for request, rdict in store.info().items():
            self.assertEqual(request, 'foo')
        value = 1
        store.update('foo', {'update':value})
        data = store.get('foo')
        print("### data", data)
        self.assertEqual('update' in data, True)
        self.assertEqual(data.get('update', None), value)

if __name__ == '__main__':
    unittest.main()
