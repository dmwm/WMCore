#!/usr/bin/env python

import unittest, commands

from unittest import TestCase
from WMCore_t.WMBS_t.Performance_t.SQLiteDAO_t import SQLiteDAO_t
from WMCore_t.WMBS_t.Performance_t.Subscription_t import Subscription_t
from WMCore.DAOFactory import DAOFactory

class SQLiteDAOSubscription_t(Subscription_t, SQLiteDAO_t, TestCase):
    """
    __SQLiteDAOSubscription_t__

     DB Performance testcase for WMBS Subscription class


    """

    def setUp(self):

        SQLiteDAO_t.setUp(self)
        Subscription_t.setUp(self,sqlURI=self.sqlURI, logarg='SQLite')
        #Set the specific threshold for the testm
        self.threshold = 1

    def tearDown(self):
        #Call superclass tearDown method
        Subscription_t.tearDown(self)
        SQLiteDAO_t.tearDown(self)
        #DB Specific tearDown code
        

if __name__ == "__main__":
    unittest.main()
