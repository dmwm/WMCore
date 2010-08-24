#!/usr/bin/env python

import unittest, commands

from unittest import TestCase
from WMCore_t.WMBS_t.Performance_t.MySQLDAO_t import MySQLDAOTest
from WMCore_t.WMBS_t.Performance_t.Location_t import LocationTest
from WMCore.DAOFactory import DAOFactory

class MySQLDAOLocationTest(LocationTest, MySQLDAOTest, TestCase):
    """
    __MySQLDAOLocationTest__

     DB Performance testcase for WMBS Location class


    """

    def setUp(self):

        MySQLDAOTest.setUp(self)
        LocationTest.setUp(self, sqlURI=self.sqlURI, logarg='MySQL')
        #Set the specific threshold for the testm
        self.threshold = 1

    def tearDown(self):
        #Call superclass tearDown method
        LocationTest.tearDown(self)
        MySQLDAOTest.tearDown(self)
        #DB Specific tearDown code
        

if __name__ == "__main__":
    unittest.main()
