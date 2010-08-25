#!/usr/bin/env python

import unittest
import logging

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
        self.dbsTestUrl = "http://cmssrv49.fnal.gov:8989/DBS/servlet/DBSServlet"
        self.testNode = "TX_Test1_Buffer"
        self.testNode2 = "TX_Test2_Buffer"

    def testInjection(self):
        dict = {}
        dict['endpoint'] = self.phedexTestDS
        dict['method'] = 'POST'
        phedexApi = PhEDEx(dict)

        print phedexApi.injectBlocks(self.dbsTestUrl, self.testNode, "/Cosmics/Sryu_Test/RAW")

    def testSubscription(self):
        dict = {}
        dict['endpoint'] = self.phedexTestDS
        dict['method'] = 'POST'
        phedexApi = PhEDEx(dict)
        print phedexApi.injectBlocks(self.dbsTestUrl, self.testNode, "/Cosmics/Sryu_Test/RAW")
        print phedexApi.injectBlocks(self.dbsTestUrl, self.testNode, "/Cosmics/Sryu_Test/RECO")

        sub1 = PhEDExSubscription("/Cosmics/Sryu_Test/RAW", self.testNode2, "TestOperator")
        sub2 = PhEDExSubscription("/Cosmics/Sryu_Test/RECO", self.testNode2, "TestOperator")
        subList = SubscriptionList()
        subList.addSubscription(sub1)
        subList.addSubscription(sub2)
        print subList
        for sub in subList.getSubscriptionList():
            print sub
            print phedexApi.subscribe(self.dbsTestUrl, sub)

if __name__ == '__main__':

    unittest.main()
