#!/usr/bin/env python

import unittest, commands

from unittest import TestCase
from WMCore_t.WMBS_t.Performance_t.SQLiteDAO_t import SQLiteDAO_t
from WMCore_t.WMBS_t.Performance_t.Workflow_t import Workflow_t
from WMCore.DAOFactory import DAOFactory

class SQLiteDAOWorkflow_t(Workflow_t, SQLiteDAO_t, TestCase):
    """
    __SQLiteDAOWorkflow_t__

     DB Performance testcase for WMBS File class


    """

    def setUp(self):

        SQLiteDAO_t.setUp(self)
        Workflow_t.setUp(self,sqlURI=self.sqlURI, logarg='SQLite')
        #Set the specific threshold for the testm
        self.threshold = 1

    def tearDown(self):
        #Call superclass tearDown method
        Workflow_t.tearDown(self)
        SQLiteDAO_t.tearDown(self)
        #DB Specific tearDown code
        

if __name__ == "__main__":
    unittest.main()
