#!/usr/bin/env python

import unittest

from unittest import TestCase
from WMCore_t.WMBS_t.Performance_t.MySQLDAO_t import MySQLDAOTest
from WMCore_t.WMBS_t.Performance_t.Fileset_t import FilesetTest
from WMCore.DAOFactory import DAOFactory

class MySQLDAOFileset_t(FilesetTest,MySQLDAOTest,TestCase):
    """
    __MySQLDAOFilesetTest__

     DB Performance testcase for WMBS Fileset class


    """

    def setUp(self):

        MySQLDAOTest.setUp(self)
        FilesetTest.setUp(self, sqlURI=self.sqlURI, logarg='MySQL')
        #Set the specific threshold for the testm
        self.threshold = 1

    def tearDown(self):
        #Call superclass tearDown method
        FilesetTest.tearDown(self)
        MySQLDAOTest.tearDown(self)
        #DB Specific tearDown code

if __name__ == "__main__":
    unittest.main()
