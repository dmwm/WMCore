#!/usr/bin/env python

"""
__MySQLDAOWorkflowTest__

DB Performance testcase for WMBS Workflow class


"""
import unittest

from unittest import TestCase
from WMCore_t.WMBS_t.Performance_t.MySQLDAO_t import MySQLDAOTest
from WMCore_t.WMBS_t.Performance_t.Workflow_t import WorkflowTest

class MySQLDAOWorkflowTest(WorkflowTest, MySQLDAOTest, TestCase):
    """
    __MySQLDAOWorkflowTest__

     DB Performance testcase for WMBS Workflow class


    """

    def setUp(self):
        """
        Specific MySQL DAO WMBS Workflow object Testcase setUp    

        """
        MySQLDAOTest.setUp(self)
        WorkflowTest.setUp(self, sqlURI=self.sqlURI, logarg='MySQL')

    def tearDown(self):
        """
        Specific MySQL DAO WMBS Workflow object Testcase tearDown

        """
        #Call superclass tearDown method
        WorkflowTest.tearDown(self)
        MySQLDAOTest.tearDown(self)
        #DB Specific tearDown code
        

if __name__ == "__main__":
    unittest.main()

