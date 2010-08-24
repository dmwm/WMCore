#!/usr/bin/env python

import unittest, commands

from unittest import TestCase
from WMCore_t.WMBS_t.Performance_t.SQLiteDAO_t import SQLiteDAOTest
from WMCore_t.WMBS_t.Performance_t.Workflow_t import WorkflowTest
from WMCore.DAOFactory import DAOFactory

class SQLiteDAOWorkflowTest(WorkflowTest, SQLiteDAOTest, TestCase):
    """
    __SQLiteDAOWorkflowTest__

     DB Performance testcase for WMBS File class


    """

    def setUp(self):

        SQLiteDAOTest.setUp(self)
        WorkflowTest.setUp(self,sqlURI=self.sqlURI, logarg='SQLite')
        #Set the specific threshold for the testm
        self.threshold = 1

    def tearDown(self):
        #Call superclass tearDown method
        WorkflowTest.tearDown(self)
        SQLiteDAOTest.tearDown(self)
        #DB Specific tearDown code
        

if __name__ == "__main__":
    unittest.main()
