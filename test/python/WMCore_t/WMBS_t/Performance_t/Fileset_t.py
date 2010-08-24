#!/usr/bin/env python

import logging
from WMCore_t.WMBS_t.Performance_t.Base_t import Base_t
from WMCore.Database.DBFactory import DBFactory

class Fileset_t(Base_t):
    """
    __Fileset_t__

     Performance testcase for WMBS Fileset class

     This class is abstract, proceed to the DB specific testcase
     to run the test


    """
    def setUp(self, sqlURI='', logarg=''):
        #Call common setUp method from Base_t
                
        self.logger = logging.getLogger(logarg + 'FilesetPerformanceTest')
        
        dbf = DBFactory(self.logger, sqlURI)
        
        Base_t.setUp(self,dbf=dbf)

    def tearDown(self):
        #Call superclass tearDown method
        Base_t.tearDown(self)

    def testNew(self):
        print "testNew"

        time = self.perfTest(dao=self.dao, action='Fileset.New', execinput=['name=self.testFileset.name'])
        assert time <= self.threshold, 'New DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'   

    def testDelete(self):
        print "testDelete"

        time = self.perfTest(dao=self.dao, action='Fileset.Delete', execinput=['name=self.testFileset.name'])
        assert time <= self.threshold, 'Delete DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'   

    def testExists(self):
        print "testExists"

        time = self.perfTest(dao=self.dao, action='Fileset.Exists', execinput=['name=self.testFileset.name'])
        assert time <= self.threshold, 'Exists DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'   

    def testLoadFromID(self):
        print "testLoadFromID"

        time = self.perfTest(dao=self.dao, action='Fileset.LoadFromID', execinput=['fileset=self.testFileset.id'])
        assert time <= self.threshold, 'LoadFromID DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'  

    def testLoadFromName(self):
        print "testLoadFromName"

        time = self.perfTest(dao=self.dao, action='Fileset.LoadFromName', execinput=['fileset=self.testFileset.name'])
        assert time <= self.threshold, 'LoadFromName DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'  

