#!/usr/bin/env python

import unittest

from unittest import TestCase
from WMCore_t.WMBS_t.Performance_t.MySQLDAO_t import MySQLDAO_t
from WMCore_t.WMBS_t.Performance_t.Fileset_t import Fileset_t
from WMCore.DAOFactory import DAOFactory

class MySQLDAOFileset_t(Fileset_t,MySQLDAO_t,TestCase):
    """
    __MySQLDAOFileset_t__

     DB Performance testcase for WMBS Fileset class


    """

    def setUp(self):

        MySQLDAO_t.setUp(self)
        Fileset_t.setUp(self, sqlURI=self.sqlURI, logarg='MySQL')
        #Set the specific threshold for the testm
        self.threshold = 1

    def tearDown(self):
        #Call superclass tearDown method
        Fileset_t.tearDown(self)
        MySQLDAO_t.tearDown(self)
        #DB Specific tearDown code

if __name__ == "__main__":
    unittest.main()
