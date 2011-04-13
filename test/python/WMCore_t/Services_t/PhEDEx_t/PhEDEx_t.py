#!/usr/bin/env python
"""
_PhEDEx_t_

Unit test for the PhEDEx helper class.  These tests need to be run by someone
that has inject permissions.
"""

import unittest
import time

from WMCore.Services.UUID import makeUUID
from WMCore.Services.DBS import XMLDrop
from WMCore.Services.PhEDEx.PhEDEx import PhEDEx 
from WMCore.Services.PhEDEx.DataStructs.SubscriptionList import PhEDExSubscription
from WMCore.Services.PhEDEx.DataStructs.SubscriptionList import SubscriptionList

from nose.plugins.attrib import attr

class PhEDExTest(unittest.TestCase):

    def setUp(self):
        """
        _setUp_
        
        Initialize the PhEDEx API to point at the test server.
        """
        phedexTestDS = "https://cmsweb.cern.ch/phedex/datasvc/json/test"
        self.dbsTestUrl = "http://vocms09.cern.ch:8880/cms_dbs_int_local_yy_writer/servlet/DBSServlet"
        self.phedexApi = PhEDEx({"endpoint": phedexTestDS,
                                 "method": "POST"})
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
        xmlData = XMLDrop.makePhEDExXMLForDatasets(self.dbsTestUrl, 
                                                   testSub.getDatasetPaths())
        result = self.phedexApi.subscribe(testSub, xmlData)
        requestIDs = result["phedex"]["request_created"]

        self.assertEqual(len(requestIDs), 1,
                         "Error: Wrong number of request IDs")
        self.assertTrue(requestIDs[0].has_key("id"),
                        "Error: Missing request ID")
        return

    @attr("integration")
    def testNodeMap(self):
        """
        _testNodeMap_

        Verify that the node map can be retrieve from PhEDEx and that the
        getNodeSE() and getNodeNames() methods work correctly.
        """
        self.failUnless(self.phedexApi.getNodeSE("T2_FR_GRIF_LLR") == "polgrid4.in2p3.fr")
        self.failUnless(self.phedexApi.getNodeNames("cmssrm.fnal.gov") == ["T1_US_FNAL_Buffer",
                                                                           "T1_US_FNAL_MSS"])
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

        injectionSpec = XMLDrop.XMLInjectionSpec(self.dbsTestUrl)
        datasetSpec = injectionSpec.getDataset(testDataset)
        datasetSpec.getFileblock(blockA, True)
        datasetSpec.getFileblock(blockB, True)
        improv = injectionSpec.save()
        self.phedexApi.injectBlocks("T1_US_FNAL_MSS", improv.makeDOMElement().toprettyxml())

        # Create a dataset level subscription to a node
        testDatasetSub = PhEDExSubscription([testDataset], "T1_UK_RAL_MSS",
                                            "Saturn", requestOnly = "n")
        xmlData = XMLDrop.makePhEDExXMLForDatasets(self.dbsTestUrl, 
                                                   testDatasetSub.getDatasetPaths())
        self.phedexApi.subscribe(testDatasetSub, xmlData)

        # Create a block level subscrtion to a different node
        testBlockSub = PhEDExSubscription([testDataset], "T1_DE_KIT_MSS", "Saturn",
                                          level = "block", requestOnly = "n")
        self.phedexApi.subscribe(testBlockSub, improv.makeDOMElement().toprettyxml())

        subs = self.phedexApi.getSubscriptionMapping(testDataset)
        self.assertEqual(subs[testDataset], set(["T1_UK_RAL_MSS"]),
                         "Error: Dataset subscription is wrong.")

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

    @attr('integration')
    def testAuth(self):
        """
        _testAuth_

        Verify that the auth method works correctly."
        """
        self.assertFalse(self.phedexApi.getAuth("datasvc_whatever"))
        self.assertTrue(self.phedexApi.getAuth("datasvc_subscribe"))
        self.assertTrue(self.phedexApi.getAuth("datasvc_inject"))

        return

if __name__ == '__main__':
    unittest.main()
