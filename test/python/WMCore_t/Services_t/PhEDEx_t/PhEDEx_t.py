#!/usr/bin/env python
"""
_PhEDEx_t_

Unit test for the PhEDEx helper class.  These tests need to be run by someone
that has inject permissions.
"""

import unittest
from nose.plugins.attrib import attr

import WMCore.Services.PhEDEx.XMLDrop as XMLDrop
from WMCore.Services.PhEDEx.DataStructs.SubscriptionList import PhEDExSubscription
from WMCore.Services.PhEDEx.PhEDEx import PhEDEx
from WMCore.Services.UUIDLib import makeUUID
from WMQuality.Emulators.EmulatedUnitTestCase import EmulatedUnitTestCase


DSET = "/HIHardProbes/HIRun2018-v1/RAW"
BLOCK = "/HIHardProbes/HIRun2018-v1/RAW#1a4c93dc-07f7-43d7-8458-5508a046a588"


class PhEDExTest(EmulatedUnitTestCase):
    def setUp(self):
        """
        _setUp_

        Initialize the PhEDEx API to point at the test server.
        """
        self.dbsTestUrl = "https://cmsweb.cern.ch/dbs/prod/global/DBSReader"
        self.phedexApi = PhEDEx()

        return

    @attr("integration")
    def testInjection(self):
        """
        _testInjection_

        Verify that we can inject data into PhEDEx.
        """
        xmlData = XMLDrop.makePhEDExDrop(self.dbsTestUrl, makeUUID())
        result = self.phedexApi.injectBlocks("T1_US_FNAL_MSS", xmlData)
        self.assertEqual(result["phedex"]["injected"],
                         {"stats": {"closed_datasets": 0, "closed_blocks": 0,
                                    "new_blocks": 0, "new_datasets": 1,
                                    "new_files": 0}})
        return

    @attr("integration")
    def testSubscription(self):
        """
        _testSubscription_

        Verify that the subscription API works.
        """
        datasetA = "/%s/WMCorePhEDExTest/RAW" % makeUUID()
        datasetB = "/%s/WMCorePhEDExTest/RECO" % makeUUID()
        xmlData = XMLDrop.makePhEDExDrop(self.dbsTestUrl, datasetA)
        self.phedexApi.injectBlocks("T1_US_FNAL_MSS", xmlData)
        xmlData = XMLDrop.makePhEDExDrop(self.dbsTestUrl, datasetB)
        self.phedexApi.injectBlocks("T1_US_FNAL_MSS", xmlData)

        testSub = PhEDExSubscription([datasetA, datasetB], "T1_UK_RAL_MSS",
                                     "Saturn")
        result = self.phedexApi.subscribe(testSub)
        requestIDs = result["phedex"]["request_created"]

        self.assertEqual(len(requestIDs), 1,
                         "Error: Wrong number of request IDs")
        self.assertTrue("id" in requestIDs[0],
                        "Error: Missing request ID")
        return

    @attr('integration')
    def testGetSubscriptionMapping(self):
        """
        _testGetSubscriptionMapping_

        Verify that the subscription mapping API works correctly.
        """
        testDataset = "/%s/WMCorePhEDExTest/RECO" % makeUUID()
        blockA = "%s#%s" % (testDataset, makeUUID())
        blockB = "%s#%s" % (testDataset, makeUUID())

        # NOTE: leaving it broken on purpose, we do NOT want to subscribe
        # data via unit tests :-)
        #injectionSpec = XMLDrop.XMLInjectionSpec(self.dbsTestUrl)
        datasetSpec = injectionSpec.getDataset(testDataset)
        datasetSpec.getFileblock(blockA, 'y')
        datasetSpec.getFileblock(blockB, 'y')
        blockSpec = injectionSpec.save()
        self.phedexApi.injectBlocks("T1_US_FNAL_MSS", blockSpec)

        # Create a dataset level subscription to a node
        testDatasetSub = PhEDExSubscription([testDataset], "T1_UK_RAL_MSS",
                                            "Saturn", request_only="y")
        self.phedexApi.subscribe(testDatasetSub)

        # Create a block level subscrtion to a different node
        testBlockSub = PhEDExSubscription([testDataset], "T1_DE_KIT_MSS", "Saturn",
                                          level="block", request_only="y")
        self.phedexApi.subscribe(testBlockSub)

        subs = self.phedexApi.getSubscriptionMapping(testDataset)
        self.assertEqual(subs[testDataset], {"T1_UK_RAL_MSS"}, "Error: Dataset subscription is wrong.")

        subs = self.phedexApi.getSubscriptionMapping(blockA)
        self.assertEqual(len(subs[blockA]), 2,
                         "Error: Wrong number of nodes in block subscription.")
        self.assertTrue("T1_UK_RAL_MSS" in subs[blockA],
                        "Error: RAL missing from block sub.")
        self.assertTrue("T1_DE_KIT_MSS" in subs[blockA],
                        "Error: KIT missing from block sub.")
        return

    def testPFNLookup(self):
        """
        _testPFNLookup_

        Verify that the PFN lookup in PhEDEx works correctly.
        """
        call1 = self.phedexApi.getPFN(['T2_UK_SGrid_Bristol'], ['/store/user/metson/file'])

        # Should get one mapping back (one lfn, one node)
        self.assertTrue(len(call1.keys()) == 1)
        call1_key = call1.keys()[0]

        call2 = self.phedexApi.getPFN(['T2_UK_SGrid_Bristol', 'T1_US_FNAL_Buffer'], ['/store/user/metson/file'])
        # Should get back two mappings (two nodes)
        self.assertTrue(call1_key in call2.keys())

        # and one of the mappings should be the same as from the previous call
        self.assertTrue(call1[call1_key] == call2[call1_key])
        return

    def testGetReplicaInfoForBlocks(self):
        """
        Test `getReplicaInfoForBlocks` method, the ability to retrieve replica
        locations provided a (or a list of) datasets and blocks
        """
        def _checkOutcome(numFiles, replica):
            "run the checks"
            if rep['complete'] == 'y':
                self.assertEqual(rep['files'], numFiles)
            if rep['custodial'] == 'y':
                self.assertTrue(rep['node'].endswith("_MSS"))
                self.assertTrue(rep['subscribed'], 'y')

        replicaDict = {'bytes', 'complete', 'custodial', 'files', 'group',
                       'node', 'node_id', 'se', 'subscribed',
                       'time_create', 'time_update'}

        res = self.phedexApi.getReplicaInfoForBlocks(block=BLOCK)['phedex']
        self.assertEqual(len(res['block']), 1)
        self.assertEqual(res['block'][0]['name'], BLOCK)
        self.assertTrue(len(res['block'][0]['replica']) > 1)
        self.assertItemsEqual(res['block'][0]['replica'][0].keys(), replicaDict)
        numFiles = res['block'][0]['files']
        for rep in res['block'][0]['replica']:
            _checkOutcome(numFiles, rep)

        # same test, but providing a dataset as input (which has only the block above)
        res = self.phedexApi.getReplicaInfoForBlocks(dataset=DSET)['phedex']
        self.assertEqual(len(res['block']), 4)
        self.assertTrue(BLOCK in [blk['name'] for blk in res['block']])
        for block in res['block']:
            numFiles = block['files']
            for rep in block['replica']:
                self.assertTrue(len(block['replica']) > 1)
                _checkOutcome(numFiles, rep)

        # same test again, but providing both block and dataset
        # NOTE the PhEDEx service only process the block input, the
        # dataset argument is completely ignored
        res = self.phedexApi.getReplicaInfoForBlocks(dataset=DSET, block=BLOCK)['phedex']
        self.assertEqual(len(res['block']), 1)
        self.assertEqual(res['block'][0]['name'], BLOCK)
        self.assertTrue(len(res['block'][0]['replica']) > 1)
        self.assertItemsEqual(res['block'][0]['replica'][0].keys(), replicaDict)
        numFiles = res['block'][0]['files']
        for rep in res['block'][0]['replica']:
            _checkOutcome(numFiles, rep)

        # provide a block that does not exist
        res = self.phedexApi.getReplicaInfoForBlocks(dataset=DSET, block=BLOCK + "BLAH")['phedex']
        self.assertTrue(res['block'] == [])

    def testGroupUsage(self):
        """
        _testGroupUsage_

        Verify that the `getGroupUsage` API works correctly.
        """
        node = "T2_DE_DESY"
        group = "DataOps"
        res = self.phedexApi.getGroupUsage(group=group, node=node)['phedex']
        self.assertEqual(len(res['node']), 1)
        self.assertEqual(len(res['node'][0]['group']), 1)
        self.assertEqual(res['node'][0]['group'][0]['name'], group)
        self.assertEqual(res['node'][0]['name'], node)
        self.assertTrue(res['node'][0]['group'][0]['dest_bytes'] > 100)

        res = self.phedexApi.getGroupUsage(group=group)['phedex']
        self.assertTrue(len(res['node']) > 50)
        self.assertEqual(len(res['node'][10]['group']), 1)
        self.assertEqual(res['node'][10]['group'][0]['name'], group)

        return


if __name__ == '__main__':
    unittest.main()
