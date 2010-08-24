#!/usr/bin/env python

"""
__MySQLDAOLocationTest__

DB Performance testcase for WMBS Location class


"""
import unittest

from unittest import TestCase
from WMCore_t.WMBS_t.Performance_t.MySQLDAO_t import MySQLDAOTest
from WMCore_t.WMBS_t.Performance_t.Location_t import LocationTest
from nose.plugins.attrib import attr
class MySQLDAOLocationTest(LocationTest, MySQLDAOTest, TestCase):
    __performance__=True
    """
    __MySQLDAOLocationTest__

     DB Performance testcase for WMBS Location class


    """

    def setUp(self):
        """
        Specific MySQL DAO WMBS Location object Testcase setUp

        """
        MySQLDAOTest.setUp(self)
        LocationTest.setUp(self, sqlURI=self.sqlURI, logarg='MySQL')

    def tearDown(self):
        """
        Specific MySQL DAO WMBS Location object Testcase tearDown

        """
        #Call superclass tearDown method
        LocationTest.tearDown(self)
        MySQLDAOTest.tearDown(self)
        #DB Specific tearDown code
        

if __name__ == "__main__":
    unittest.main()
