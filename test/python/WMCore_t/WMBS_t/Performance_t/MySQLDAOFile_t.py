#!/usr/bin/env python

"""
__MySQLDAOFileTest__

DB Performance testcase for WMBS File class


"""
import unittest

from unittest import TestCase
from WMCore_t.WMBS_t.Performance_t.MySQLDAO_t import MySQLDAOTest
from WMCore_t.WMBS_t.Performance_t.File_t import PerformanceFileTest
from nose.plugins.attrib import attr
class MySQLDAOFile_t(PerformanceFileTest, MySQLDAOTest, TestCase):
    __performance__=True
    """
    __MySQLDAOFileTest__

     DB Performance testcase for WMBS File class


    """

    def setUp(self):
        """
        Specific MySQL DAO WMBS File object Testcase setUp    

        """
        MySQLDAOTest.setUp(self)
        PerformanceFileTest.setUp(self, sqlURI=self.sqlURI, logarg='MySQL')

    def tearDown(self):
        """
        Specific MySQL DAO WMBS File object Testcase tearDown    

        """
        #Call superclass tearDown method
        PerformanceFileTest.tearDown(self)
        MySQLDAOTest.tearDown(self)
        #DB Specific tearDown code
        

if __name__ == "__main__":
    unittest.main()
