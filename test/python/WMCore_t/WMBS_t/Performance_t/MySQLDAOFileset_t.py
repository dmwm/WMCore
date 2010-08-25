#!/usr/bin/env python

"""
__MySQLDAOFilesetTest__

DB Performance testcase for WMBS Fileset class


"""

import unittest

from unittest import TestCase
from WMCore_t.WMBS_t.Performance_t.MySQLDAO_t import MySQLDAOTest
from WMCore_t.WMBS_t.Performance_t.Fileset_t import FilesetTest
from nose.plugins.attrib import attr
class MySQLDAOFileset_t(FilesetTest, MySQLDAOTest, TestCase):
    __performance__=True
    """
    __MySQLDAOFilesetTest__

     DB Performance testcase for WMBS Fileset class


    """

    def setUp(self):
        """
        Specific MySQL DAO WMBS Fileset object Testcase setUp    

        """
        MySQLDAOTest.setUp(self)
        FilesetTest.setUp(self, sqlURI=self.sqlURI, logarg='MySQL')

    def tearDown(self):
        """
        Specific MySQL DAO WMBS Fileset object Testcase tearDown    

        """
        #Call superclass tearDown method
        FilesetTest.tearDown(self)
        MySQLDAOTest.tearDown(self)
        #DB Specific tearDown code

if __name__ == "__main__":
    unittest.main()
