#!/usr/bin/env python

import unittest, commands

from unittest import TestCase
from WMCore_t.WMBS_t.Performance_t.SQLiteDAO_t import SQLiteDAOTest
from WMCore_t.WMBS_t.Performance_t.Subscription_t import SubscriptionTest
from WMCore.DAOFactory import DAOFactory

class SQLiteDAOSubscriptionTest(SubscriptionTest, SQLiteDAOTest, TestCase):
    """
    __SQLiteDAOSubscriptionTest__

     DB Performance testcase for WMBS Subscription class


    """

    def setUp(self):

        SQLiteDAOTest.setUp(self)
        SubscriptionTest.setUp(self,sqlURI=self.sqlURI, logarg='SQLite')
        #Set the specific threshold for the testm
        self.threshold = 1

    def tearDown(self):
        #Call superclass tearDown method
        SubscriptionTest.tearDown(self)
        SQLiteDAOTest.tearDown(self)
        #DB Specific tearDown code
        

if __name__ == "__main__":
    unittest.main()
