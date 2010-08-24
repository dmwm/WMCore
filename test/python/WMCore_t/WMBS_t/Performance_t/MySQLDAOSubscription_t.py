#!/usr/bin/env python

"""
__MySQLDAOSubscriptionTest__

DB Performance testcase for WMBS Subscription class


"""
import unittest

from unittest import TestCase
from WMCore_t.WMBS_t.Performance_t.MySQLDAO_t import MySQLDAOTest
from WMCore_t.WMBS_t.Performance_t.Subscription_t import SubscriptionTest
from nose.plugins.attrib import attr
class MySQLDAOSubscriptionTest(SubscriptionTest, MySQLDAOTest, TestCase):
    __performance__=True
    """
    __MySQLDAOSubscriptionTest__

     DB Performance testcase for WMBS Subscription class


    """

    def setUp(self):
        """
        Specific MySQL DAO WMBS Subscription object Testcase setUp    

        """
        MySQLDAOTest.setUp(self)
        SubscriptionTest.setUp(self, sqlURI=self.sqlURI, logarg='MySQL')

    def tearDown(self):
        """
        Specific MySQL DAO WMBS Subscription object Testcase tearDown    

        """
        #Call superclass tearDown method
        SubscriptionTest.tearDown(self)
        MySQLDAOTest.tearDown(self)
        #DB Specific tearDown code
        

if __name__ == "__main__":
    unittest.main()
