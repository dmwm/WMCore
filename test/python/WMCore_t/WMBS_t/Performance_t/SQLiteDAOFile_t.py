#!/usr/bin/env python

"""
__SQLiteDAOFileTest__

DB Performance testcase for WMBS File class


"""
import unittest

from unittest import TestCase
from WMCore_t.WMBS_t.Performance_t.SQLiteDAO_t import SQLiteDAOTest
from WMCore_t.WMBS_t.Performance_t.File_t import PerformanceFileTest as FileTest
from nose.plugins.attrib import attr
class SQLiteDAOFileTest(FileTest, SQLiteDAOTest, TestCase):
    __performance__=True
    """
    __SQLiteDAOFileTest__

     DB Performance testcase for WMBS File class


    """

    def setUp(self):
        """
        Specific SQLite DAO WMBS File object Testcase setUp

        """
        SQLiteDAOTest.setUp(self)
        FileTest.setUp(self, sqlURI=self.sqlURI, logarg='SQLite')

    def tearDown(self):
        """
        Specific SQLite DAO WMBS File object Testcase tearDown

        """
        #Call superclass tearDown method
        FileTest.tearDown(self)
        SQLiteDAOTest.tearDown(self)
        #DB Specific tearDown code
        

if __name__ == "__main__":
    unittest.main()
