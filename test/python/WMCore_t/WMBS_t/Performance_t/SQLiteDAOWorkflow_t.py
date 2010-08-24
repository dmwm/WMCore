#!/usr/bin/env python

"""
__SQLiteDAOWorkflowTest__

DB Performance testcase for WMBS File class


"""
import unittest

from unittest import TestCase
from WMCore_t.WMBS_t.Performance_t.SQLiteDAO_t import SQLiteDAOTest
from WMCore_t.WMBS_t.Performance_t.Workflow_t import WorkflowTest
from nose.plugins.attrib import attr
class SQLiteDAOWorkflowTest(WorkflowTest, SQLiteDAOTest, TestCase):
    __performance__=True
    """
    __SQLiteDAOWorkflowTest__

     DB Performance testcase for WMBS File class


    """

    def setUp(self):
        """
        Specific SQLite DAO WMBS Workflow object Testcase setUp

        """
        SQLiteDAOTest.setUp(self)
        WorkflowTest.setUp(self, sqlURI=self.sqlURI, logarg='SQLite')

    def tearDown(self):
        """
        Specific SQLite DAO WMBS Workflow object Testcase tearDown

        """
        #Call superclass tearDown method
        WorkflowTest.tearDown(self)
        SQLiteDAOTest.tearDown(self)
        #DB Specific tearDown code
        

if __name__ == "__main__":
    unittest.main()
