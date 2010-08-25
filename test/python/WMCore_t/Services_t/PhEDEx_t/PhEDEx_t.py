#!/usr/bin/env python

import unittest
import logging

from WMCore.Services.DBS import XMLDrop
from WMCore.Services.PhEDEx.PhEDEx import PhEDEx 
from WMCore.Services.PhEDEx.DataStructs.SubscriptionList import PhEDExSubscription
from WMCore.Services.PhEDEx.DataStructs.SubscriptionList import SubscriptionList


class PhEDExTest(unittest.TestCase):
    """
    Provide setUp and tearDown for Reader package module

    """
    def setUp(self):
        """
        setUP global values
        """
        #dsUrl = "http://cmswttest.cern.ch:7701/phedex/datasvc/xml/tbedi/"
        #dsUrl = "https://cmsweb.cern.ch/phedex/datasvc/xml/tbedi/"
        #self.phedexTestDS = "http://cmswttest.cern.ch/phedex/datasvc/xml/tbedi/"
        self.phedexTestDS = "https://cmswttest.cern.ch/phedex/datasvc/json/tbedi/"

        #To check your authorithy to access
        #https://cmswttest.cern.ch/phedex/datasvc/perl/tbedi/auth?ability=datasvc_subscribe

        #self.phedexTestDS = "https://localhost:9999/phedex/datasvc/xml/tbedi/"
        self.dbsTestUrl = "http://cmssrv49.fnal.gov:8989/DBS209/servlet/DBSServlet"
        self.testNode = "TX_Test1_Buffer"
        self.testNode2 = "TX_Test2_Buffer"

    def testInjection(self):
        dict = {}
        dict['endpoint'] = self.phedexTestDS
        dict['method'] = 'POST'
        phedexApi = PhEDEx(dict)

        xmlData = XMLDrop.makePhEDExDrop(self.dbsTestUrl, "/Cosmics/Sryu_Test/RAW")
        print phedexApi.injectBlocks(self.testNode, xmlData)

    def testSubscription(self):
        dict = {}
        dict['endpoint'] = self.phedexTestDS
        dict['method'] = 'POST'
        phedexApi = PhEDEx(dict)
        xmlData = XMLDrop.makePhEDExDrop(self.dbsTestUrl, "/Cosmics/Sryu_Test/RAW")
        print phedexApi.injectBlocks(self.testNode, xmlData)
        xmlData = XMLDrop.makePhEDExDrop(self.dbsTestUrl, "/Cosmics/Sryu_Test/RECO")
        print phedexApi.injectBlocks(self.testNode, xmlData)
        
        sub1 = PhEDExSubscription("/Cosmics/Sryu_Test/RAW", self.testNode2, "TestOperator")
        sub2 = PhEDExSubscription("/Cosmics/Sryu_Test/RECO", self.testNode2, "TestOperator")
        subList = SubscriptionList()
        subList.addSubscription(sub1)
        subList.addSubscription(sub2)
        print subList
        for sub in subList.getSubscriptionList():
            print sub
            xmlData = XMLDrop.makePhEDExXMLForDatasets(self.dbsTestUrl, 
                                    newSubscription.getDatasetPaths())
            print phedexApi.subscribe(sub, xmlData)

    def testNodeMap(self):

        dict = {}
        dict['endpoint'] = self.phedexTestDS
        phedexApi = PhEDEx(dict)
        self.failUnless(phedexApi.getNodeSE('TX_Test4_MSS') == 'srm.test4.ch')
        self.failUnless(phedexApi.getNodeNames('srm.test1.ch') == [u'TX_Test1_MSS', u'TX_Test1_Buffer'])

    def testGetSubscriptionMapping(self):
        dataset = '/MinimumBias/BeamCommissioning09-v1/RAW'
        blocks = [dataset + '#0bd096b1-0022-42df-a0d8-a8e6346080b8',
                  dataset + '#0beea1ae-8029-455c-9139-5acd6ba3b949']
        node = 'T0_CH_CERN_MSS'
        dict = {}
        #dict['endpoint'] = self.phedexTestDS
        phedexApi = PhEDEx(dict)

        # dataset level subscriptions
        subs = phedexApi.getSubscriptionMapping(dataset)
        self.assert_(node in subs[dataset])
        subs = phedexApi.getSubscriptionMapping(*blocks)
        self.assert_(node in subs[blocks[0]])
        self.assert_(node in subs[blocks[1]])
        # block level subscription
        block = '/MinBias/Summer09-MC_31X_V3_7TeV-v1/GEN-SIM-RAW#814604df-507e-4354-9130-4d9f8c0cfc29'
        subs = phedexApi.getSubscriptionMapping(block)
        self.assert_('T2_UK_London_IC' in subs[block])
        #TODO: Add test for dataset with block level subscriptions

if __name__ == '__main__':

    unittest.main()
