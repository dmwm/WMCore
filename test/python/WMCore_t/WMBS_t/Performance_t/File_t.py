#!/usr/bin/env python

import logging
from WMCore_t.WMBS_t.Performance_t.Base_t import Base_t
from WMCore.Database.DBFactory import DBFactory

class File_t(Base_t):
    """
    __File_t__

     Performance testcase for WMBS File class

     This class is abstract, proceed to the DB specific testcase
     to run the test


    """
    
    def setUp(self, sqlURI='', logarg=''):
        #Call common setUp method from Base_t
                
        self.logger = logging.getLogger(logarg + 'FilePerformanceTest')
        
        dbf = DBFactory(self.logger, sqlURI)
        
        Base_t.setUp(self,dbf=dbf)

    def tearDown(self):
        #Call superclass tearDown method
        Base_t.tearDown(self)

    def testAdd(self):
        print "testAdd"
        # Can't reuse testfile here - it's already in the database
        lfn = "/store/user/testfile0001"
        size = 25168286
        events = 10000
        time = self.perfTest(dao=self.dao, action='Files.Add', files=str(lfn), size=size, events=events)
        assert time <= self.threshold, 'Add DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testAddRunLumi(self): 
        print "testAddRunLumi"
        # Can't reuse testfile here - it's already in the database
        lfn = "/store/user/testfile0001"
        run = 1234
        lumi = 8
        time = self.perfTest(dao=self.dao, action='Files.AddRunLumi', files=str(lfn), run=run, lumi=lumi)
        assert time <= self.threshold, 'AddRunLumi DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testAddToFileset(self):
        print "testAddToFileset"
        
        time = self.perfTest(dao=self.dao, action='Files.AddToFileset', file=self.testFile['lfn'], fileset=self.testFileset.name)
        assert time <= self.threshold, 'AddToFileset DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testDelete(self):
        print "testDelete"
        
        time = self.perfTest(dao=self.dao, action='Files.Delete', file=self.testFile["lfn"])
        assert time <= self.threshold, 'Delete DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testGetByID(self):
        print "testGetByID"
        
        time = self.perfTest(dao=self.dao, action='Files.GetByID', files=self.testFile["id"])
        assert time <= self.threshold, 'GetByID DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testGetByLFN(self):
        print "testGetByLFN"
        
        time = self.perfTest(dao=self.dao, action='Files.GetByLFN', files=self.testFile["lfn"])
        assert time <= self.threshold, 'GetByLFN DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testGetLocation(self):
        print "testGetLocation"
        
        time = self.perfTest(dao=self.dao, action='Files.GetLocation', file=self.testFile['lfn'])
        assert time <= self.threshold, 'GetLocation DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testGetParents(self):
        print "testGetParents"
                
        time = self.perfTest(dao=self.dao, action='Files.GetParents', files=self.testFile['lfn'])
        assert time <= self.threshold, 'GetParents DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'


#    def testHeritage(self):
#        self.dao(classname='Files.Heritage')                
#        #TODO - parent and child argument settings
#        time = self.perfTest(dao=self.dao, execinput='parent= , child= '+self.baseexec)
#        assert time <= self.threshold, 'Heritage DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testInFileset(self):
        print "testInFileset"
        
        time = self.perfTest(dao=self.dao, action='Files.InFileset', fileset=self.testFileset.id)#+self.baseexec)
        assert time <= self.threshold, 'InFileset DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'

    def testSetLocation(self):
        print "testSetLocation"

        time = self.perfTest(dao=self.dao, action='Files.SetLocation', file=self.testFile["lfn"], sename=self.selist[0])#+self.baseexec)
        assert time <= self.threshold, 'SetLocation DAO class - Operation too slow ( elapsed time:'+str(time)+', threshold:'+str(self.threshold)+' )'


if __name__ == "__main__":
    unittest.main()
