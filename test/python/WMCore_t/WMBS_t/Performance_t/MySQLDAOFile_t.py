#!/usr/bin/env python

import unittest, commands

from unittest import TestCase
from WMCore_t.WMBS_t.Performance_t.MySQLDAO_t import MySQLDAOTest
from WMCore_t.WMBS_t.Performance_t.File_t import FileTest
from WMCore.DAOFactory import DAOFactory

class MySQLDAOFile_t(FileTest, MySQLDAOTest, TestCase):
    """
    __MySQLDAOFileTest__

     DB Performance testcase for WMBS File class


    """

    def setUp(self):

        MySQLDAOTest.setUp(self)
        FileTest.setUp(self, sqlURI=self.sqlURI, logarg='MySQL')
        #Set the specific threshold for the test
        self.threshold = 1

    def tearDown(self):
        #Call superclass tearDown method
        FileTest.tearDown(self)
        MySQLDAOTest.tearDown(self)
        #DB Specific tearDown code
        

if __name__ == "__main__":
    unittest.main()
