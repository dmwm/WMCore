#!/usr/bin/env python

import unittest

from unittest import TestCase
from WMCore_t.WMBS_t.Performance_t.Base_t import Base_t


class Fileset_t(Base_t,TestCase):
    """
    __Fileset_t__

     DB Performance testcase for WMBS Fileset class


    """

    def setUp(self):
        #Call common setUp method from Base_t
        Base_t.setUp(self)
        self.threshold = 1

    def tearDown(self):
        #Call superclass tearDown method
        Base_t.tearDown(self)

    def testNew(self):
        print "testNew"

        time = self.perfTest(dao=self.mysqldao, action='Fileset.New', execinput=['name=self.testmysqlFileset.name'])
        assert time <= self.threshold, 'New DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'   

    def testDelete(self):
        print "testDelete"

        time = self.perfTest(dao=self.mysqldao, action='Fileset.Delete', execinput=['name=self.testmysqlFileset.name'])
        assert time <= self.threshold, 'Delete DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'   

    def testExists(self):
        print "testExists"

        time = self.perfTest(dao=self.mysqldao, action='Fileset.Exists', execinput=['name=self.testmysqlFileset.name'])
        assert time <= self.threshold, 'Exists DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'   

    def testLoadFromID(self):
        print "testLoadFromID"

        time = self.perfTest(dao=self.mysqldao, action='Fileset.LoadFromID', execinput=['fileset=self.testmysqlFileset.id'])
        assert time <= self.threshold, 'LoadFromID DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'  

    def testLoadFromName(self):
        print "testLoadFromName"

        time = self.perfTest(dao=self.mysqldao, action='Fileset.LoadFromName', execinput=['fileset=self.testmysqlFileset.name'])
        assert time <= self.threshold, 'LoadFromName DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'  

if __name__ == "__main__":
    unittest.main()
