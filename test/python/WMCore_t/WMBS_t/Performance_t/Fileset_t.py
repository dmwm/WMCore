#!/usr/bin/env python

import unittest, time, random

from unittest import TestCase
from WMCore_t.WMBS_t.Performance_t.Base_t import Base_t
from WMCore.WMBS.File import File
from WMCore.WMBS.Fileset import Fileset

class Fileset_t(Base_t,TestCase):
    """
    __Fileset_t__

     DB Performance testcase for WMBS Fileset class


    """

    def setUp(self):
        #Call common setUp method from Base_t
        Base_t.setUp(self)
        self.threshold = 1

        # Create a Fileset of random, parentless, childless, unlocatied file
        mysqlfilelist = []
        sqlitefilelist = []
        
        #Generating Files - MySQL DBFactory        
        for x in range(random.randint(1000,3000)):
            file = File(lfn='/store/data/%s/%s/file.root' % (random.randint(1000, 9999), 
                                                  random.randint(1000, 9999)),
                        size=random.randint(1000, 2000),
                        events = 1000,
                        run = random.randint(0, 2000),
                        lumi = random.randint(0, 8), 
                        logger=self.logger, 
                        dbfactory=self.mysqldbf)
            
            mysqlfilelist.append(file)

        #Generating Files - SQLite DBFactory        
        for x in range(random.randint(1000,3000)):
            file = File(lfn='/store/data/%s/%s/file.root' % (random.randint(1000, 9999), 
                                                  random.randint(1000, 9999)),
                        size=random.randint(1000, 2000),
                        events = 1000,
                        run = random.randint(0, 2000),
                        lumi = random.randint(0, 8), 
                        logger=self.logger, 
                        dbfactory=self.sqlitedbf)
            
            sqlitefilelist.append(file)
        
        #Creating mySQL Fileset        
        self.testmysqlFileset = Fileset(name='testFileSet', 
                            files=mysqlfilelist, 
                            logger=self.logger, 
                            dbfactory=self.mysqldbf) 
        self.testmysqlFileset.create() 
    
        #Creating SQLite Fileset        
        self.testsqliteFileset = Fileset(name='testFileSet', 
                            files=mysqlfilelist, 
                            logger=self.logger, 
                            dbfactory=self.sqlitedbf) 
        self.testsqliteFileset.create()    

    def tearDown(self):
        #Call superclass tearDown method
        Base_t.tearDown(self)

    def testNew(self):
        print "testNew"
        print "MySQL Test"        
        time = self.perfTest(dao=self.mysqldao, action='Fileset.New', execinput=['name=self.testmysqlFileset.name'])
        assert time <= self.threshold, 'New DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'   

        print "SQLite Test"        
        time = self.perfTest(dao=self.sqlitedao, action='Fileset.New', execinput=['name=self.testsqliteFileset.name'])
        assert time <= self.threshold, 'New DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'   

    def testDelete(self):
        print "testDelete"

        print "MySQL Test"
        time = self.perfTest(dao=self.mysqldao, action='Fileset.Delete', execinput=['name=self.testmysqlFileset.name'])
        assert time <= self.threshold, 'Delete DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'   

        print "SQLite Test"
        time = self.perfTest(dao=self.sqlitedao, action='Fileset.Delete', execinput=['name=self.testsqliteFileset.name'])
        assert time <= self.threshold, 'Delete DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'   

    def testExists(self):
        print "testExists"

        print "MySQL Test"
        time = self.perfTest(dao=self.mysqldao, action='Fileset.Exists', execinput=['name=self.testmysqlFileset.name'])
        assert time <= self.threshold, 'Exists DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'   

        print "SQLite Test"
        time = self.perfTest(dao=self.sqlitedao, action='Fileset.Exists', execinput=['name=self.testsqliteFileset.name'])
        assert time <= self.threshold, 'Exists DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'   

    def testLoadFromID(self):
        print "testLoadFromID"

        print "MySQL Test"
        time = self.perfTest(dao=self.mysqldao, action='Fileset.LoadFromID', execinput=['fileset=self.testmysqlFileset.id'])
        assert time <= self.threshold, 'LoadFromID DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'  

        print "SQLite Test"
        time = self.perfTest(dao=self.sqlitedao, action='Fileset.LoadFromID', execinput=['fileset=self.testsqliteFileset.id'])
        assert time <= self.threshold, 'LoadFromID DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'  

    def testLoadFromName(self):
        print "testLoadFromName"

        print "MySQL Test"
        time = self.perfTest(dao=self.mysqldao, action='Fileset.LoadFromName', execinput=['fileset=self.testmysqlFileset.name'])
        assert time <= self.threshold, 'LoadFromName DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'  

        print "SQLite Test"
        time = self.perfTest(dao=self.sqlitedao, action='Fileset.LoadFromName', execinput=['fileset=self.testsqliteFileset.name'])
        assert time <= self.threshold, 'LoadFromName DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'  

if __name__ == "__main__":
    unittest.main()
