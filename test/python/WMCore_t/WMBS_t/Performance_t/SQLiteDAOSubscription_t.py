#!/usr/bin/env python

"""
__SQLiteDAOSubscriptionTest__

DB Performance testcase for WMBS Subscription class


"""
import unittest

from unittest import TestCase
from WMCore_t.WMBS_t.Performance_t.SQLiteDAO_t import SQLiteDAOTest
from WMCore_t.WMBS_t.Performance_t.Subscription_t import SubscriptionTest
from nose.plugins.attrib import attr
class SQLiteDAOSubscriptionTest(SubscriptionTest, SQLiteDAOTest, TestCase):
    __performance__=True
    """
    __SQLiteDAOSubscriptionTest__

     DB Performance testcase for WMBS Subscription class


    """

    def setUp(self):
        """
        Specific SQLite DAO WMBS Subscription object Testcase setUp

        """
        SQLiteDAOTest.setUp(self)
        SubscriptionTest.setUp(self, sqlURI=self.sqlURI, logarg='SQLite')

    def tearDown(self):
        """
        Specific SQLite DAO WMBS Subscription object Testcase tearDown

        """
        #Call superclass tearDown method
        SubscriptionTest.tearDown(self)
        SQLiteDAOTest.tearDown(self)
        #DB Specific tearDown code
        

if __name__ == "__main__":
    unittest.main()
