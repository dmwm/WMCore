#!/usr/bin/env python

import unittest, commands

from unittest import TestCase
from WMCore_t.WMBS_t.Performance_t.SQLiteDAO_t import SQLiteDAO_t
from WMCore_t.WMBS_t.Performance_t.JobGroup_t import JobGroup_t
from WMCore.DAOFactory import DAOFactory

class SQLiteDAOJobGroup_t(JobGroup_t, SQLiteDAO_t, TestCase):
    """
    __SQLiteDAOJobGroup_t__

     DB Performance testcase for WMBS File class


    """

    def setUp(self):

        SQLiteDAO_t.setUp(self)
        JobGroup_t.setUp(self,sqlURI=self.sqlURI, logarg='SQLite')
        #Set the specific threshold for the testm
        self.threshold = 1

    def tearDown(self):
        #Call superclass tearDown method
        JobGroup_t.tearDown(self)
        SQLiteDAO_t.tearDown(self)
        #DB Specific tearDown code
        

if __name__ == "__main__":
    unittest.main()
