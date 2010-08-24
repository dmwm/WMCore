#!/usr/bin/env python

"""
__SQLiteDAOLocation_t__

DB Performance testcase for WMBS Location class


"""
import unittest

from unittest import TestCase
from WMCore_t.WMBS_t.Performance_t.SQLiteDAO_t import SQLiteDAOTest
from WMCore_t.WMBS_t.Performance_t.Location_t import LocationTest

class SQLiteDAOLocation_t(LocationTest, SQLiteDAOTest, TestCase):
    """
    __SQLiteDAOLocation_t__

     DB Performance testcase for WMBS Location class


    """

    def setUp(self):
        """
        Specific SQLite DAO WMBS Location object Testcase setUp

        """
        SQLiteDAOTest.setUp(self)
        LocationTest.setUp(self, sqlURI=self.sqlURI, logarg='SQLite')

    def tearDown(self):
        """
        Specific SQLite DAO WMBS Location object Testcase tearDown

        """
        #Call superclass tearDown method
        LocationTest.tearDown(self)
        SQLiteDAOTest.tearDown(self)
        #DB Specific tearDown code
        

if __name__ == "__main__":
    unittest.main()
