#!/usr/bin/env python

import unittest, commands

from unittest import TestCase
from WMCore_t.WMBS_t.Performance_t.MySQLDAO_t import MySQLDAOTest
from WMCore_t.WMBS_t.Performance_t.Subscription_t import SubscriptionTest
from WMCore.DAOFactory import DAOFactory

class MySQLDAOSubscriptionTest(SubscriptionTest, MySQLDAOTest, TestCase):
    """
    __MySQLDAOSubscriptionTest__

     DB Performance testcase for WMBS Subscription class


    """

    def setUp(self):

        MySQLDAOTest.setUp(self)
        SubscriptionTest.setUp(self, sqlURI=self.sqlURI, logarg='MySQL')
        #Set the specific threshold for the testm
        self.threshold = 1

    def tearDown(self):
        #Call superclass tearDown method
        SubscriptionTest.tearDown(self)
        MySQLDAOTest.tearDown(self)
        #DB Specific tearDown code
        

if __name__ == "__main__":
    unittest.main()
