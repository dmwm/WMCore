#!/usr/bin/env python

import unittest

from unittest import TestCase
from WMCore_t.WMBS_t.Performance_t.Base_t import Base_t
from WMCore_t.WMBS_t.Performance_t.File_t import File_t
from WMCore.DAOFactory import DAOFactory

class MySQLDAOFile_t(File_t,TestCase):
    """
    __MySQLDAOFile_t__

     DB Performance testcase for WMBS File class


    """

    def setUp(self):
        #Call common setUp method from Base_t
        mysqlURI = 'mysql://jcg@localhost/wmbs'
        
        File_t.setUp(self,sqlURI=mysqlURI, logname='MySQLFilePerformanceTest')
        #Set the specific threshold for the test
        self.threshold = 1

    def tearDown(self):
        #Call superclass tearDown method
        File_t.tearDown(self)


if __name__ == "__main__":
    unittest.main()
