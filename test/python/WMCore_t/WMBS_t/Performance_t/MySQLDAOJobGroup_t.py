#!/usr/bin/env python
"""
__MySQLDAOJobGroupTest__

DB Performance testcase for WMBS JobGroup class


"""
import unittest

from unittest import TestCase
from WMCore_t.WMBS_t.Performance_t.MySQLDAO_t import MySQLDAOTest
from WMCore_t.WMBS_t.Performance_t.JobGroup_t import JobGroupTest

class MySQLDAOJobGroupTest(JobGroupTest, MySQLDAOTest, TestCase):
    """
    __MySQLDAOJobGroupTest__

     DB Performance testcase for WMBS JobGroup class


    """

    def setUp(self):
        """
        Specific MySQL DAO WMBS JobGroup object Testcase setUp

        """
        MySQLDAOTest.setUp(self)
        JobGroupTest.setUp(self, sqlURI=self.sqlURI, logarg='MySQL')

    def tearDown(self):
        """
        Specific MySQL DAO WMBS JobGroup object Testcase tearDown

        """
        #Call superclass tearDown method
        JobGroupTest.tearDown(self)
        MySQLDAOTest.tearDown(self)
        #DB Specific tearDown code
        

if __name__ == "__main__":
    unittest.main()
