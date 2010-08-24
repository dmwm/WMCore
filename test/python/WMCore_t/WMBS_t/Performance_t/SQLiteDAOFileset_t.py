#!/usr/bin/env python

"""
__SQLiteDAOFilesetTest__

DB Performance testcase for WMBS Fileset class


"""
import unittest

from unittest import TestCase
from WMCore_t.WMBS_t.Performance_t.SQLiteDAO_t import SQLiteDAOTest
from WMCore_t.WMBS_t.Performance_t.Fileset_t import FilesetTest

class SQLiteDAOFilesetTest(FilesetTest, SQLiteDAOTest, TestCase):
    """
    __SQLiteDAOFilesetTest__

     DB Performance testcase for WMBS Fileset class


    """

    def setUp(self):
        """
        Specific SQLite DAO WMBS Fileset object Testcase setUp

        """
        SQLiteDAOTest.setUp(self)
        FilesetTest.setUp(self, sqlURI=self.sqlURI, logarg='SQLite')

    def tearDown(self):
        """
        Specific SQLite DAO WMBS Fileset object Testcase tearDown

        """
        #Call superclass tearDown method
        FilesetTest.tearDown(self)
        SQLiteDAOTest.tearDown(self)
        #DB Specific tearDown code
        

if __name__ == "__main__":
    unittest.main()
