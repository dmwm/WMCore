#!/usr/bin/env python

"""
__SQLiteDAOJobGroupTest__

DB Performance testcase for WMBS JobGroup class


"""
import unittest

from unittest import TestCase
from WMCore_t.WMBS_t.Performance_t.SQLiteDAO_t import SQLiteDAOTest
from WMCore_t.WMBS_t.Performance_t.JobGroup_t import JobGroupTest

class SQLiteDAOJobGroupTest(JobGroupTest, SQLiteDAOTest, TestCase):
    """
    __SQLiteDAOJobGroupTest__

     DB Performance testcase for WMBS JobGroup class


    """

    def setUp(self):
        """
        Specific SQLite DAO WMBS JobGroup object Testcase setUp

        """
        SQLiteDAOTest.setUp(self)
        JobGroupTest.setUp(self, sqlURI=self.sqlURI, logarg='SQLite')

    def tearDown(self):
        """
        Specific SQLite DAO WMBS JobGroup object Testcase tearDown

        """
        #Call superclass tearDown method
        JobGroupTest.tearDown(self)
        SQLiteDAOTest.tearDown(self)
        #DB Specific tearDown code
        

if __name__ == "__main__":
    unittest.main()
