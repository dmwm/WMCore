#!/usr/bin/env python

import unittest, commands

from unittest import TestCase
from WMCore_t.WMBS_t.Performance_t.MySQLDAO_t import MySQLDAO_t
from WMCore_t.WMBS_t.Performance_t.Workflow_t import Workflow_t
from WMCore.DAOFactory import DAOFactory

class MySQLDAOWorkflow_t(Workflow_t, MySQLDAO_t, TestCase):
    """
    __MySQLDAOWorkflow_t__

     DB Performance testcase for WMBS Workflow class


    """

    def setUp(self):

        MySQLDAO_t.setUp(self)
        Workflow_t.setUp(self, sqlURI=self.sqlURI, logarg='MySQL')
        #Set the specific threshold for the testm
        self.threshold = 1

    def tearDown(self):
        #Call superclass tearDown method
        Workflow_t.tearDown(self)
        MySQLDAO_t.tearDown(self)
        #DB Specific tearDown code
        

if __name__ == "__main__":
    unittest.main()

