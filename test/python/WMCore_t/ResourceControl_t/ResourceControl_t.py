#!/usr/bin/env python

"""
_ResourceControl_t_

Unit tests for ResourceControl.

"""

__revision__ = "$Id: ResourceControl_t.py,v 1.2 2009/10/13 23:06:11 meloam Exp $"
__version__ = "$Revision: 1.2 $"

import unittest
import threading
import time
import os

from WMCore.ThreadPool.ThreadPool import ThreadPool
from WMCore.WMFactory             import WMFactory

from WMCore.ResourceControl.ResourceControl import ResourceControl

from WMQuality.TestInit import TestInit


class ResourceControlTest(unittest.TestCase):


    def setUp(self):
        """
        Standard setUp sequence
        """

        myThread = threading.currentThread()
        myThread.dialect = os.getenv("DIALECT")
        myThread.transaction = None

        self.testInit = TestInit(__file__)
        self.testInit.setLogging()
        self.testInit.setDatabaseConnection()
        #self.tearDown()
        self.testInit.setSchema(customModules = ["WMCore.ResourceControl"], useDefault = False)


        return


    def tearDown(self):
        """
        Standard tearDown sequence
        """
        myThread = threading.currentThread()

        factory = WMFactory("ResourceControl", "WMCore.ResourceControl")
        destroy = factory.loadObject(myThread.dialect + ".Destroy")
        myThread.transaction.begin()
        destroyworked = destroy.execute(conn = myThread.transaction.conn)
        if not destroyworked:
            raise Exception("Could not complete ResourceControl tear down.")
        myThread.transaction.commit()

        return


    def testA_doNothing(self):
        """
        This does nothing

        """

        myThread = threading.currentThread()

        resourceControl = ResourceControl()

        resourceControl.insertSite(siteName = 'verdun', seName = 'Petain', ceName = 'Neville')

        result = myThread.dbi.processData("SELECT site_name FROM rc_site")[0].fetchall()
        self.assertEqual(result, [('verdun',)])

        resourceControl.insertThreshold(thresholdName = 'sillyThreshold', thresholdValue = 100, siteNames = 'verdun')

        result = myThread.dbi.processData("SELECT threshold_name, threshold_value FROM rc_site_threshold")[0].fetchall()
        self.assertEqual(result, [('sillyThreshold', 100L)])

        result = resourceControl.getThresholds(siteNames = 'verdun')
        self.assertEqual(result[0], {'threshold_value': 100L, 'threshold_name': 'sillyThreshold', 'site_name': 'verdun'})

        #Placeholder to make sure tables build

        return


if __name__ == '__main__':
    unittest.main()
