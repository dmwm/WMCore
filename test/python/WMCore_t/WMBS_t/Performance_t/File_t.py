#!/usr/bin/env python

import unittest, time, random

from unittest import TestCase
from WMCore_t.WMBS_t.Performance_t.Base_t import Base_t
from WMCore.WMBS.File import File
from WMCore.WMBS.Fileset import Fileset

class File_t(Base_t,TestCase):
    """
    __File_t__

     DB Performance testcase for WMBS File class


    """

    def setUp(self):
        #Call common setUp method from Base_t
        Base_t.setUp(self)
        self.threshold = 1
        # Create a File to be used as argument for the performance test
        file_lfn = '/tmp/file/fileexample'
        file_events = 1111
        file_size = 1111
        file_run = 111
        file_lumi = 0
        #Create File - MySQL
        self.testmysqlFile = File(lfn=file_lfn, size=file_size, events=file_events, run=file_run,
                    lumi=file_lumi, logger=self.logger, dbfactory=self.mysqldbf)
        self.testmysqlFile.save()
        
        #Create File - SQLite
        self.testsqliteFile = File(lfn=file_lfn, size=file_size, events=file_events, run=file_run,
                    lumi=file_lumi, logger=self.logger, dbfactory=self.sqlitedbf)
        self.testsqliteFile.save()

        #Extra Debugging        
        #testtuple = self.testmysqlFile.getInfo()
        #print testtuple
        #assert testtuple[1] > 0,'MySQL Test ID error - ID is Negative: %d' % testtuple[1]

        #testtuple = self.testsqliteFile.getInfo()
        #print testtuple
        #assert testtuple[1] > 0,'SQLite Test ID error - ID is Negative: %d' % testtuple[1]

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

    def testAdd(self):
        print "testAdd"
        print self.testFile['lfn'], self.testFile['size'], self.testFile['events']

        print "MySQL Test"
        time = self.perfTest(dao=self.mysqldao, action='Files.Add', execinput=['files=self.testmysqlFile["lfn"]', 'size=self.testmysqlFile["size"]', 'events=self.testmysqlFile["events"]'])
        assert time <= self.threshold, 'Add DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

        print "SQLite Test"
        time = self.perfTest(dao=self.sqlitedao, action='Files.Add', execinput=['files=self.testsqliteFile["lfn"]', 'size=self.testsqliteFile["size"]', 'events=self.testsqliteFile["events"]'])
        assert time <= self.threshold, 'Add DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testAddRunLumi(self): 
        print "testAddRunLumi"

        print "MySQL Test"        
        time = self.perfTest(dao=self.mysqldao, action='Files.AddRunLumi', execinput=['files=self.testmysqlFileset.name', 'run=self.run', 'lumi=self.lumi'])
        assert time <= self.threshold, 'AddRunLumi DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

        print "SQLite Test"        
        time = self.perfTest(dao=self.sqlitedao, action='Files.AddRunLumi', execinput=['files=self.testsqliteFileset.name', 'run=self.run', 'lumi=self.lumi'])
        assert time <= self.threshold, 'AddRunLumi DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testAddToFileset(self):
        print "testAddToFileset"

        print "MySQL Test"
        time = self.perfTest(dao=self.mysqldao, action='Files.AddToFileset', execinput=['file=self.testmysqlFile.lfn', 'fileset=self.testmysqlFileset.name '])
        assert time <= self.threshold, 'AddToFileset DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

        print "SQLite Test"
        time = self.perfTest(dao=self.sqlitedao, action='Files.AddToFileset', execinput=['file=self.testsqliteFile.lfn', 'fileset=self.testsqliteFileset.name '])
        assert time <= self.threshold, 'AddToFileset DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testDelete(self):
        print "testDelete"

        print "MySQL Test"
        time = self.perfTest(dao=self.mysqldao, action='Files.Delete', execinput=['file=self.testmysqlFile'])
        assert time <= self.threshold, 'Delete DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

        print "SQLite Test"
        time = self.perfTest(dao=self.sqlitedao, action='Files.Delete', execinput=['file=self.testsqliteFile'])
        assert time <= self.threshold, 'Delete DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testGetByID(self):
        print "testGetByID"

        print "MySQL Test"
        time = self.perfTest(dao=self.mysqldao, action='Files.GetByID', execinput=['files=self.testmysqlFile.id'])
        assert time <= self.threshold, 'GetByID DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

        print "SQLite Test"
        time = self.perfTest(dao=self.sqlitedao, action='Files.GetByID', execinput=['files=self.testsqliteFile.id'])
        assert time <= self.threshold, 'GetByID DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testGetByLFN(self):
        print "testGetByLFN"

        print "MySQL Test" 
        time = self.perfTest(dao=self.mysqldao, action='Files.GetByLFN', execinput=['files=self.testmysqlFile.lfn'])
        assert time <= self.threshold, 'GetByLFN DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

        print "SQLite Test" 
        time = self.perfTest(dao=self.sqlitedao, action='Files.GetByLFN', execinput=['files=self.testsqliteFile.lfn'])
        assert time <= self.threshold, 'GetByLFN DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testGetLocation(self):
        print "testGetLocation"

        print "MySQL Test"
        time = self.perfTest(dao=self.mysqldao, action='Files.GetLocation', execinput=['files=self.testmysqlFileset'])
        assert time <= self.threshold, 'GetLocation DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

        print "SQLite Test"
        time = self.perfTest(dao=self.sqlitedao, action='Files.GetLocation', execinput=['files=self.testsqliteFileset'])
        assert time <= self.threshold, 'GetLocation DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testGetParents(self):
        print "testGetParents"
        
        print "MySQL Test"
        time = self.perfTest(dao=self.mysqldao, action='Files.GetParents', execinput=['files=self.testmysqlFileset'])
        assert time <= self.threshold, 'GetParents DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

        print "SQLite Test"
        time = self.perfTest(dao=self.sqlitedao, action='Files.GetParents', execinput=['files=self.testsqliteFileset'])
        assert time <= self.threshold, 'GetParents DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

#    def testHeritage(self):
#        self.mysqldao(classname='Files.Heritage')                
#        #TODO - parent and child argument settings
#        time = self.perfTest(dao=self.mysqldao, execinput='parent= , child= '+self.baseexec)
#        assert time <= self.threshold, 'Heritage DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testInFileset(self):
        print "testInFileset"

        print "MySQL Test"
        time = self.perfTest(dao=self.mysqldao, action='Files.InFileset', execinput=['files=self.testmysqlFileset'])#+self.baseexec)
        assert time <= self.threshold, 'InFileset DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

        print "SQLite Test"
        time = self.perfTest(dao=self.sqlitedao, action='Files.InFileset', execinput=['files=self.testsqliteFileset'])#+self.baseexec)
        assert time <= self.threshold, 'InFileset DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testSetLocation(self):
        print "testSetLocation"

        print "MySQL Test"
        time = self.perfTest(dao=self.mysqldao, action='Files.SetLocation', execinput=['file=self.testmysqlFile', 'sename=self.sename'])#+self.baseexec)
        assert time <= self.threshold, 'SetLocation DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

        print "SQLite Test"
        time = self.perfTest(dao=self.sqlitedao, action='Files.SetLocation', execinput=['file=self.testsqliteFile', 'sename=self.sename'])#+self.baseexec)
        assert time <= self.threshold, 'SetLocation DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

if __name__ == "__main__":
    unittest.main()
