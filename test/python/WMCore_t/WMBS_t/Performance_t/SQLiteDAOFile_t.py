#!/usr/bin/env python

import unittest, commands

from unittest import TestCase
from WMCore_t.WMBS_t.Performance_t.SQLiteDAO_t import SQLiteDAOTest
from WMCore_t.WMBS_t.Performance_t.File_t import FileTest
from WMCore.DAOFactory import DAOFactory

class SQLiteDAOFileTest(FileTest, SQLiteDAOTest, TestCase):
    """
    __SQLiteDAOFileTest__

     DB Performance testcase for WMBS File class


    """

    def setUp(self):

        SQLiteDAOTest.setUp(self)
        FileTest.setUp(self,sqlURI=self.sqlURI, logarg='SQLite')
        #Set the specific threshold for the testm
        self.threshold = 1

    def tearDown(self):
        #Call superclass tearDown method
        FileTest.tearDown(self)
        SQLiteDAOTest.tearDown(self)
        #DB Specific tearDown code
        

if __name__ == "__main__":
    unittest.main()
