#!/usr/bin/env python

"""
__SQLiteDAOJobTest__

DB Performance testcase for WMBS Job class


"""
import unittest

from unittest import TestCase
from WMCore_t.WMBS_t.Performance_t.SQLiteDAO_t import SQLiteDAOTest
from WMCore_t.WMBS_t.Performance_t.Job_t import JobTest
from nose.plugins.attrib import attr
class SQLiteDAOJobTest(JobTest, SQLiteDAOTest, TestCase):
    __performance__=True
    """
    __SQLiteDAOJobTest__

     DB Performance testcase for WMBS Job class


    """

    def setUp(self):
        """
        Specific SQLite DAO WMBS Job object Testcase setUp

        """
        SQLiteDAOTest.setUp(self)
        JobTest.setUp(self, sqlURI=self.sqlURI, logarg='SQLite')

    def tearDown(self):
        """
        Specific SQLite DAO WMBS Job object Testcase setUp

        """
        #Call superclass tearDown method
        JobTest.tearDown(self)
        SQLiteDAOTest.tearDown(self)
        #DB Specific tearDown code
        

if __name__ == "__main__":
    unittest.main()
