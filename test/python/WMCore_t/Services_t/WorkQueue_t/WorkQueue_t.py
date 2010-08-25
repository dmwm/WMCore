#!/usr/bin/env python

import unittest
import logging

from WMCore.Services.WorkQueue.WorkQueue import WorkQueue

class WorkQueueServiceTest(unittest.TestCase):
    """
    Provide setUp and tearDown for Reader package module

    """
    def setUp(self):
        """
        setUP global values
        """
        wmbsTestDS = "http://cmssrv18.fnal.gov:6660/workqueue/"
        
        self.params = {}
        self.params['endpoint'] = wmbsTestDS
        
        

    def testGetWork(self):
        wqApi = WorkQueue(self.params)

        print wqApi.getWork({'SiteB' : 15, 'SiteA' : 15}, "http://test.url")

    def testSynchronize(self):
        wqApi = WorkQueue(self.params)
        childUrl = "http://test.url"
        childResources = [{'ParentQueueId' : 1, 'Status' : 'Available'}]
        print wqApi.synchronize(childUrl, childResources)
        
        childUrl = "http://test.url"
        childResources = []
        print wqApi.synchronize(childUrl, childResources)
        
if __name__ == '__main__':

    unittest.main()