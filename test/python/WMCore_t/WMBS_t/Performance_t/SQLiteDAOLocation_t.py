#!/usr/bin/env python

import unittest, commands

from unittest import TestCase
from WMCore_t.WMBS_t.Performance_t.SQLiteDAO_t import SQLiteDAOTest
from WMCore_t.WMBS_t.Performance_t.Location_t import LocationTest
from WMCore.DAOFactory import DAOFactory

class SQLiteDAOLocation_t(LocationTest, SQLiteDAOTest, TestCase):
    """
    __SQLiteDAOLocation_t__

     DB Performance testcase for WMBS Location class


    """

    def setUp(self):

        SQLiteDAOTest.setUp(self)
        LocationTest.setUp(self, sqlURI=self.sqlURI, logarg='SQLite')
        #Set the specific threshold for the testm
        self.threshold = 1

    def tearDown(self):
        #Call superclass tearDown method
        LocationTest.tearDown(self)
        SQLiteDAOTest.tearDown(self)
        #DB Specific tearDown code
        

if __name__ == "__main__":
    unittest.main()
