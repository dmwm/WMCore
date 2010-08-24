#!/usr/bin/env python

import unittest, commands

from unittest import TestCase
from WMCore_t.WMBS_t.Performance_t.SQLiteDAO_t import SQLiteDAO_t
from WMCore_t.WMBS_t.Performance_t.Fileset_t import Fileset_t
from WMCore.DAOFactory import DAOFactory

class SQLiteDAOFile_t(Fileset_t, SQLiteDAO_t, TestCase):
    """
    __SQLiteDAOFileset_t__

     DB Performance testcase for WMBS Fileset class


    """

    def setUp(self):

        SQLiteDAO_t.setUp(self)
        Fileset_t.setUp(self,sqlURI=self.sqlURI, logarg='SQLite')
        #Set the specific threshold for the testm
        self.threshold = 1

    def tearDown(self):
        #Call superclass tearDown method
        Fileset_t.tearDown(self)
        SQLiteDAO_t.tearDown(self)
        #DB Specific tearDown code
        

if __name__ == "__main__":
    unittest.main()
