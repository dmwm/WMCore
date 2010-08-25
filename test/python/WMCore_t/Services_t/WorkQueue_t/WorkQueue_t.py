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
        self.wmbsTestDS = "http://cmssrv18.fnal.gov:6660/workqueue/"
        

    def testGetWork(self):
        dict = {}
        dict['endpoint'] = self.wmbsTestDS
        wqApi = WorkQueue(dict)

        print wqApi.getWork({'SiteB' : 15, 'SiteA' : 15}, "http://test.url")
        
if __name__ == '__main__':

    unittest.main()