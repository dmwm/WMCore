#!/usr/bin/env python
"""
__MySQLDAOJobTest__

DB Performance testcase for WMBS Job class


"""
import unittest

from unittest import TestCase
from WMCore_t.WMBS_t.Performance_t.MySQLDAO_t import MySQLDAOTest
from WMCore_t.WMBS_t.Performance_t.Job_t import JobTest

class MySQLDAOJobTest(JobTest, MySQLDAOTest, TestCase):
    """
    __MySQLDAOJobTest__

     DB Performance testcase for WMBS Job class


    """

    def setUp(self):
        """
        Specific MySQL DAO WMBS Job object Testcase setUp    

        """
        MySQLDAOTest.setUp(self)
        JobTest.setUp(self, sqlURI=self.sqlURI, logarg='MySQL')

    def tearDown(self):
        """
        Specific MySQL DAO WMBS Job object Testcase tearDown    

        """
        #Call superclass tearDown method
        JobTest.tearDown(self)
        MySQLDAOTest.tearDown(self)
        #DB Specific tearDown code
        

if __name__ == "__main__":
    unittest.main()
